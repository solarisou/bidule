"""
Injection M3
Responsable : SC
"""

import xml.etree.ElementTree as ET
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"         # a changer

XML_CLIENTS    = "../vente_referentiel_clients.xml"
XML_OPERATIONS = "../vente_suivi_operations.xml"

# ─────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ══════════════════════════════════════════
# FICHIER 1 — referentiel_clients.xml
# ══════════════════════════════════════════
def inject_clients(tx, tree):
    root = tree.getroot()

    for client in root.findall("client"):
        identite = client.find("identite")
        contact  = client.find("contact")

        tx.run("""
            MERGE (c:Client {id: $id})
            SET c.nom       = $nom,
                c.prenom    = $prenom,
                c.adresse   = $adresse,
                c.email     = $email,
                c.telephone = $telephone,
                c.service   = 'Vente'
        """,
            id        = client.get("id"),
            nom       = identite.findtext("nom", "")       if identite else "",
            prenom    = identite.findtext("prenom", "")    if identite else "",
            adresse   = contact.findtext("adresse", "")    if contact  else "",
            email     = contact.findtext("email", "")      if contact  else "",
            telephone = contact.findtext("telephone", "")  if contact  else "",
        )
        print(f"  Client injecté : {client.get('id')} - {identite.findtext('nom') if identite else ''}")


# ══════════════════════════════════════════
# FICHIER 2 — suivi_operations.xml
# ══════════════════════════════════════════
def inject_operations(tx, tree):
    root = tree.getroot()

    for dossier in root.findall("dossier"):
        client_ref = dossier.get("client_ref")

        # ── DEMANDE ───────────────────────────────────────────────────────────
        demande = dossier.find("demande")
        if demande is not None:
            tx.run("""
                MERGE (d:Demande {id: $id})
                SET d.date_reception = $date,
                    d.type_bateau    = $type_bateau,
                    d.statut         = $statut,
                    d.service        = 'Vente'
            """,
                id          = demande.get("id"),
                date        = demande.findtext("date_reception", ""),
                type_bateau = demande.findtext("type_bateau", ""),
                statut      = demande.findtext("statut", ""),
            )
            # Lien Client → Demande (inter-fichier via client_ref)
            tx.run("""
                MATCH (c:Client  {id: $id_client})
                MATCH (d:Demande {id: $id_demande})
                MERGE (c)-[:A_SOUMIS]->(d)
            """,
                id_client  = client_ref,
                id_demande = demande.get("id"),
            )
            print(f"  Demande injectée : {demande.get('id')} → Client {client_ref}")

        # ── DEVIS ─────────────────────────────────────────────────────────────
        devis = dossier.find("devis")
        if devis is not None:
            fin = devis.find("details_financiers")
            log = devis.find("logistique")

            tx.run("""
                MERGE (dv:Devis {id: $id})
                SET dv.montant_ht           = toFloat($ht),
                    dv.montant_ttc          = toFloat($ttc),
                    dv.prix_revient_calcule = toFloat($prix_rev),
                    dv.delai_fabrication_j  = toInteger($delai),
                    dv.date_validite        = $validite,
                    dv.service              = 'Vente'
            """,
                id      = devis.get("id"),
                ht      = fin.get("ht", "0")          if fin else "0",
                ttc     = fin.get("ttc", "0")         if fin else "0",
                prix_rev= fin.get("prix_revient", "0") if fin else "0",
                delai   = log.get("delai", "0")       if log else "0",
                validite= log.get("validite", "")     if log else "",
            )

            # Lien Demande → Devis
            if demande is not None:
                tx.run("""
                    MATCH (d:Demande {id: $id_demande})
                    MATCH (dv:Devis  {id: $id_devis})
                    MERGE (d)-[:A_GENERE]->(dv)
                """,
                    id_demande = demande.get("id"),
                    id_devis   = devis.get("id"),
                )

            # Matières → lien inter-service M1 (Stock)
            for matiere in devis.findall("estimation_matieres/matiere"):
                ref      = matiere.get("ref")
                quantite = float(matiere.get("quantite", 0))
                unite    = matiere.get("unite", "")
                prix_u   = float(matiere.get("prix_unitaire", 0))

                tx.run("""
                    MERGE (m:MatierePremiere {code: $code})
                    ON CREATE SET m.note = 'Référencé depuis Vente'
                """, code=ref)

                tx.run("""
                    MATCH (dv:Devis          {id:   $id_devis})
                    MATCH (m:MatierePremiere {code: $code})
                    MERGE (dv)-[r:UTILISE]->(m)
                    SET r.quantite      = $quantite,
                        r.unite         = $unite,
                        r.prix_unitaire = $prix_u
                """,
                    id_devis = devis.get("id"),
                    code     = ref,
                    quantite = quantite,
                    unite    = unite,
                    prix_u   = prix_u,
                )
                print(f"    ↳ Matière liée : {ref}  ({quantite} {unite})")

            print(f"  Devis injecté  : {devis.get('id')}")

        # ── COMMANDE ──────────────────────────────────────────────────────────
        commande = dossier.find("commande")
        if commande is not None:
            reglement = commande.find("reglement")

            tx.run("""
                MERGE (cmd:Commande {id: $id})
                SET cmd.date_commande         = $date_cmd,
                    cmd.date_livraison_prevue  = $date_liv,
                    cmd.statut                = $statut,
                    cmd.montant_total_paye    = toFloat($montant),
                    cmd.service               = 'Vente'
            """,
                id       = commande.get("id"),
                date_cmd = commande.findtext("date_commande", ""),
                date_liv = commande.findtext("livraison_prevue", ""),
                statut   = commande.findtext("statut", ""),
                montant  = reglement.get("montant", "0") if reglement else "0",
            )

            # Lien Devis → Commande
            if devis is not None:
                tx.run("""
                    MATCH (dv:Devis     {id: $id_devis})
                    MATCH (cmd:Commande {id: $id_cmd})
                    MERGE (dv)-[:A_DONNE_LIEU_A]->(cmd)
                """,
                    id_devis = devis.get("id"),
                    id_cmd   = commande.get("id"),
                )

            # Lien Client → Commande (inter-fichier)
            tx.run("""
                MATCH (c:Client    {id: $id_client})
                MATCH (cmd:Commande {id: $id_cmd})
                MERGE (c)-[:A_PASSE]->(cmd)
            """,
                id_client = client_ref,
                id_cmd    = commande.get("id"),
            )
            print(f"  Commande injectée : {commande.get('id')}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M3 - Service Vente → Neo4j")
    print("=" * 50)

    print("\n[1/2] Fichier : vente_referentiel_clients.xml")
    tree_clients = ET.parse(XML_CLIENTS)
    with driver.session() as session:
        session.execute_write(inject_clients, tree_clients)

    print("\n[2/2] Fichier : vente_suivi_operations.xml")
    tree_ops = ET.parse(XML_OPERATIONS)
    with driver.session() as session:
        session.execute_write(inject_operations, tree_ops)

    driver.close()
    print("\nInjection Vente terminée avec succès !")
    print("\nNœuds créés : Client · Demande · Devis · Commande")
    print("Relations   : A_SOUMIS · A_GENERE · UTILISE · A_DONNE_LIEU_A · A_PASSE")
    print("Liens inter-services : Devis ─[UTILISE]─► MatierePremiere (M1 Stock)")
