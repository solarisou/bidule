"""
Injection M3 - Service Vente → Neo4j
Responsable : SC
Mode BATCH (UNWIND) — supporte les très grands volumes.
"""

import os
import xml.etree.ElementTree as ET
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Password"

_DIR           = os.path.dirname(os.path.abspath(__file__))
XML_CLIENTS    = os.path.join(_DIR, "..", "vente_referentiel_clients.xml")
XML_OPERATIONS = os.path.join(_DIR, "..", "vente_suivi_operations.xml")

BATCH = 500

# ─────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_indexes(session):
    for q in [
        "CREATE INDEX cli_id IF NOT EXISTS FOR (n:Client)   ON (n.id)",
        "CREATE INDEX cmd_id IF NOT EXISTS FOR (n:Commande) ON (n.id)",
    ]:
        session.run(q)


# ══════════════════════════════════════════
# FICHIER 1 — referentiel_clients.xml  (BATCH)
# ══════════════════════════════════════════
def parse_clients(tree):
    clients = []
    for c in tree.getroot().findall("client"):
        identite = c.find("identite")
        contact  = c.find("contact")
        clients.append({
            "id":        c.get("id"),
            "nom":       identite.findtext("nom",       "") if identite else "",
            "prenom":    identite.findtext("prenom",    "") if identite else "",
            "adresse":   contact.findtext("adresse",    "") if contact  else "",
            "email":     contact.findtext("email",      "") if contact  else "",
            "telephone": contact.findtext("telephone",  "") if contact  else "",
        })
    return clients


def inject_clients_batch(session, clients):
    total = len(clients)
    print(f"  {total} clients à injecter...")
    for idx, batch in enumerate(chunks(clients, BATCH)):
        def writer(tx, d=batch):
            tx.run("""
                UNWIND $rows AS row
                MERGE (c:Client {id: row.id})
                SET c.nom       = row.nom,
                    c.prenom    = row.prenom,
                    c.adresse   = row.adresse,
                    c.email     = row.email,
                    c.telephone = row.telephone,
                    c.service   = 'Vente'
            """, rows=d)
        session.execute_write(writer)
        print(f"  [{min((idx+1)*BATCH, total):>6}/{total}]", end="\r", flush=True)
    print(f"  [{total:>6}/{total}] ✓")


# ══════════════════════════════════════════
# FICHIER 2 — suivi_operations.xml  (BATCH + individuel pour demande/devis)
# ══════════════════════════════════════════
def parse_operations(tree):
    commandes   = []
    demandes    = []
    devis_list  = []

    for dossier in tree.getroot().findall("dossier"):
        client_ref = dossier.get("client_ref")

        # Demande (rare)
        dem = dossier.find("demande")
        if dem is not None:
            demandes.append({
                "id":          dem.get("id"),
                "client_ref":  client_ref,
                "date":        dem.findtext("date_reception", ""),
                "type_bateau": dem.findtext("type_bateau",    ""),
                "statut":      dem.findtext("statut",         ""),
            })

        # Devis (rare)
        dv = dossier.find("devis")
        if dv is not None:
            fin = dv.find("details_financiers")
            log = dv.find("logistique")
            matieres = []
            for mat in dv.findall("estimation_matieres/matiere"):
                matieres.append({
                    "code":     mat.get("ref"),
                    "quantite": float(mat.get("quantite",     0)),
                    "unite":    mat.get("unite", ""),
                    "prix_u":   float(mat.get("prix_unitaire", 0)),
                })
            devis_list.append({
                "id":        dv.get("id"),
                "dem_id":    dem.get("id") if dem is not None else None,
                "ht":        fin.get("ht",           "0") if fin else "0",
                "ttc":       fin.get("ttc",          "0") if fin else "0",
                "prix_rev":  fin.get("prix_revient", "0") if fin else "0",
                "delai":     log.get("delai",        "0") if log else "0",
                "validite":  log.get("validite",      "") if log else "",
                "matieres":  matieres,
            })

        # Commande
        cmd = dossier.find("commande")
        if cmd is not None:
            reg = cmd.find("reglement")
            commandes.append({
                "id":         cmd.get("id"),
                "client_ref": client_ref,
                "date_cmd":   cmd.findtext("date_commande",     ""),
                "date_liv":   cmd.findtext("livraison_prevue",  ""),
                "statut":     cmd.findtext("statut",            ""),
                "montant":    reg.get("montant", "0") if reg else "0",
                "devis_id":   dv.get("id") if dv is not None else None,
            })

    return commandes, demandes, devis_list


def inject_commandes_batch(session, commandes):
    total = len(commandes)
    print(f"  {total} commandes à injecter...")
    for idx, batch in enumerate(chunks(commandes, BATCH)):
        def writer(tx, d=batch):
            tx.run("""
                UNWIND $rows AS row
                MERGE (cmd:Commande {id: row.id})
                SET cmd.date_commande        = row.date_cmd,
                    cmd.date_livraison_prevue = row.date_liv,
                    cmd.statut               = row.statut,
                    cmd.montant_total_paye   = toFloat(row.montant),
                    cmd.service              = 'Vente'
                WITH cmd, row
                MATCH (c:Client {id: row.client_ref})
                MERGE (c)-[:A_PASSE]->(cmd)
            """, rows=d)
        session.execute_write(writer)
        print(f"  [{min((idx+1)*BATCH, total):>6}/{total}]", end="\r", flush=True)
    print(f"  [{total:>6}/{total}] ✓")


def inject_demandes_devis(session, demandes, devis_list):
    """Injection individuelle (peu nombreux)."""
    def writer(tx):
        for d in demandes:
            tx.run("""
                MERGE (dem:Demande {id: $id})
                SET dem.date_reception = $date, dem.type_bateau = $type_bateau,
                    dem.statut = $statut, dem.service = 'Vente'
            """, id=d["id"], date=d["date"], type_bateau=d["type_bateau"], statut=d["statut"])
            tx.run("""
                MATCH (c:Client  {id: $id_c})
                MATCH (d:Demande {id: $id_d})
                MERGE (c)-[:A_SOUMIS]->(d)
            """, id_c=d["client_ref"], id_d=d["id"])

        for dv in devis_list:
            tx.run("""
                MERGE (dv:Devis {id: $id})
                SET dv.montant_ht = toFloat($ht), dv.montant_ttc = toFloat($ttc),
                    dv.prix_revient_calcule = toFloat($prix_rev),
                    dv.delai_fabrication_j  = toInteger($delai),
                    dv.date_validite        = $validite, dv.service = 'Vente'
            """, id=dv["id"], ht=dv["ht"], ttc=dv["ttc"],
                 prix_rev=dv["prix_rev"], delai=dv["delai"], validite=dv["validite"])
            if dv["dem_id"]:
                tx.run("""
                    MATCH (d:Demande {id: $id_d}) MATCH (dv:Devis {id: $id_dv})
                    MERGE (d)-[:A_GENERE]->(dv)
                """, id_d=dv["dem_id"], id_dv=dv["id"])
            for mat in dv["matieres"]:
                tx.run("""
                    MERGE (m:MatierePremiere {code: $code})
                    ON CREATE SET m.note = 'Référencé depuis Vente'
                """, code=mat["code"])
                tx.run("""
                    MATCH (dv:Devis {id: $id_dv}) MATCH (m:MatierePremiere {code: $code})
                    MERGE (dv)-[r:UTILISE]->(m)
                    SET r.quantite = $qte, r.unite = $unite, r.prix_unitaire = $prix_u
                """, id_dv=dv["id"], code=mat["code"],
                     qte=mat["quantite"], unite=mat["unite"], prix_u=mat["prix_u"])

    session.execute_write(writer)
    print(f"  {len(demandes)} demande(s), {len(devis_list)} devis injecté(s)")


def inject_devis_commandes_links(session, commandes):
    """Lien Devis → Commande (seulement pour ceux qui ont un devis)."""
    linked = [c for c in commandes if c["devis_id"]]
    if not linked:
        return
    def writer(tx, d=linked):
        for c in d:
            tx.run("""
                MATCH (dv:Devis {id: $id_dv}) MATCH (cmd:Commande {id: $id_cmd})
                MERGE (dv)-[:A_DONNE_LIEU_A]->(cmd)
            """, id_dv=c["devis_id"], id_cmd=c["id"])
    session.execute_write(writer)


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M3 - Service Vente → Neo4j")
    print("=" * 50)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database="navalcraft") as s:
        print("\n[0] Création des index Neo4j")
        create_indexes(s)

    print("\n[1/2] Fichier : vente_referentiel_clients.xml")
    tree_clients = ET.parse(XML_CLIENTS)
    clients = parse_clients(tree_clients)
    with driver.session(database="navalcraft") as s:
        inject_clients_batch(s, clients)

    print("\n[2/2] Fichier : vente_suivi_operations.xml")
    tree_ops = ET.parse(XML_OPERATIONS)
    commandes, demandes, devis_list = parse_operations(tree_ops)

    with driver.session(database="navalcraft") as s:
        inject_commandes_batch(s, commandes)
        if demandes or devis_list:
            inject_demandes_devis(s, demandes, devis_list)
            inject_devis_commandes_links(s, commandes)

    driver.close()

    print("\nInjection Vente terminée avec succès !")
    print("\nNœuds créés : Client · Demande · Devis · Commande")
    print("Relations   : A_SOUMIS · A_GENERE · UTILISE · A_DONNE_LIEU_A · A_PASSE")
    print("Liens inter-services : Devis ─[UTILISE]─► MatierePremiere (M1 Stock)")
