"""
Injection M2 - Service Achat → Neo4j
Responsable : AM
Lit les 3 fichiers JSON et injecte les nœuds Fournisseur,
BonCommande, DemandeInfo et leurs relations dans Neo4j.
"""

import json
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"   # à changer

JSON_FOURNISSEURS  = "../achat_fournisseurs.json"
JSON_BONS_COMMANDE = "../achat_bons_commande.json"
JSON_DEMANDES_INFO = "../achat_demandes_info.json"

# ─────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ══════════════════════════════════════════
# FICHIER 1 — achat_fournisseurs.json
# ══════════════════════════════════════════
def inject_fournisseurs(tx, data):
    for f in data:
        contact     = f.get("contact", {})
        performance = f.get("performance", {})

        # Nœud Fournisseur (complète le nœud créé par inject_stock si déjà existant)
        tx.run("""
            MERGE (f:Fournisseur {id: $id})
            SET f.nom                       = $nom,
                f.telephone                 = $telephone,
                f.email                     = $email,
                f.adresse                   = $adresse,
                f.delai_livraison_moyen_j   = $delai,
                f.pays                      = $pays,
                f.service                   = 'Achat'
        """,
            id        = f["id_fournisseur"],
            nom       = f.get("nom", ""),
            telephone = contact.get("telephone", ""),
            email     = contact.get("email", ""),
            adresse   = contact.get("adresse", ""),
            delai     = performance.get("delai_livraison_moyen_jours", 0),
            pays      = performance.get("pays", ""),
        )

        # Liens Fournisseur → MatierePremiere (catalogue)
        for mat in f.get("catalogue_matieres", []):
            # Le nœud MatierePremiere est créé/complété (inject_stock l'a peut-être déjà fait)
            tx.run("""
                MERGE (m:MatierePremiere {code: $code})
                ON CREATE SET m.note = 'Référencé depuis Achat'
            """, code=mat["ref_matiere"])

            tx.run("""
                MATCH (f:Fournisseur     {id:   $id_f})
                MATCH (m:MatierePremiere {code: $code})
                MERGE (f)-[r:FOURNIT]->(m)
                SET r.prix_unitaire = $prix,
                    r.unite         = $unite
            """,
                id_f  = f["id_fournisseur"],
                code  = mat["ref_matiere"],
                prix  = float(mat.get("prix_unitaire", 0)),
                unite = mat.get("unite", ""),
            )
            print(f"    ↳ Catalogue : {f['id_fournisseur']} fournit {mat['ref_matiere']}")

        print(f"  Fournisseur injecté : {f['id_fournisseur']} — {f.get('nom', '')}")


# ══════════════════════════════════════════
# FICHIER 2 — achat_bons_commande.json
# ══════════════════════════════════════════
def inject_bons_commande(tx, data):
    for bc in data:
        tx.run("""
            MERGE (bc:BonCommande {id: $id})
            SET bc.date_commande            = $date_cmd,
                bc.statut                   = $statut,
                bc.montant_total_ht         = $ht,
                bc.montant_total_ttc        = $ttc,
                bc.date_livraison_prevue    = $date_prev,
                bc.date_livraison_effective = $date_eff,
                bc.num_bon_livraison        = $bon_liv,
                bc.commande_client_associee = $cmd_client,
                bc.service                  = 'Achat'
        """,
            id         = bc["num_bon_commande"],
            date_cmd   = bc.get("date_commande", ""),
            statut     = bc.get("statut", ""),
            ht         = float(bc.get("montant_total_ht", 0)),
            ttc        = float(bc.get("montant_total_ttc", 0)),
            date_prev  = bc.get("date_livraison_prevue", ""),
            date_eff   = bc.get("date_livraison_effective") or "",
            bon_liv    = bc.get("num_bon_livraison") or "",
            cmd_client = bc.get("commande_client_associee", ""),
        )

        # Lien BonCommande → Fournisseur
        tx.run("""
            MATCH (bc:BonCommande {id: $id_bc})
            MATCH (f:Fournisseur  {id: $id_f})
            MERGE (bc)-[:ADRESSE_A]->(f)
        """,
            id_bc = bc["num_bon_commande"],
            id_f  = bc["id_fournisseur"],
        )

        # Liens BonCommande → MatierePremiere (articles commandés)
        for art in bc.get("details_articles", []):
            tx.run("""
                MERGE (m:MatierePremiere {code: $code})
                ON CREATE SET m.note = 'Référencé depuis Achat'
            """, code=art["ref_matiere"])

            tx.run("""
                MATCH (bc:BonCommande    {id:   $id_bc})
                MATCH (m:MatierePremiere {code: $code})
                MERGE (bc)-[r:COMMANDE]->(m)
                SET r.quantite_commandee  = $qte,
                    r.prix_unitaire_achat = $prix
            """,
                id_bc = bc["num_bon_commande"],
                code  = art["ref_matiere"],
                qte   = float(art.get("quantite_commandee", 0)),
                prix  = float(art.get("prix_unitaire_achat", 0)),
            )
            print(f"    ↳ Article : {art['ref_matiere']} ({art.get('quantite_commandee')})")

        # Lien inter-service : BonCommande → Commande Vente (M3)
        cmd_client = bc.get("commande_client_associee", "")
        if cmd_client.startswith("CMD-"):
            tx.run("""
                MERGE (cmd:Commande {id: $id_cmd})
                ON CREATE SET cmd.note = 'Référencé depuis Achat'
            """, id_cmd=cmd_client)

            tx.run("""
                MATCH (bc:BonCommande {id:    $id_bc})
                MATCH (cmd:Commande   {id:    $id_cmd})
                MERGE (bc)-[:DECLENCHE_PAR]->(cmd)
            """,
                id_bc  = bc["num_bon_commande"],
                id_cmd = cmd_client,
            )

        print(f"  Bon commande injecté : {bc['num_bon_commande']} ({bc.get('statut')})")


# ══════════════════════════════════════════
# FICHIER 3 — achat_demandes_info.json
# ══════════════════════════════════════════
def inject_demandes_info(tx, data):
    for di in data:
        reponse = di.get("reponse") or {}

        tx.run("""
            MERGE (d:DemandeInfo {id: $id})
            SET d.service_demandeur                 = $service,
                d.date_demande                      = $date,
                d.objet                             = $objet,
                d.date_reponse                      = $date_rep,
                d.cout_total_estime_ht              = $cout,
                d.delai_max_approvisionnement_jours = $delai,
                d.service                           = 'Achat'
        """,
            id       = di["num_demande"],
            service  = di.get("service_demandeur", ""),
            date     = di.get("date_demande", ""),
            objet    = di.get("objet", ""),
            date_rep = reponse.get("date_reponse", ""),
            cout     = float(reponse.get("cout_total_estime_ht", 0)) if reponse else 0.0,
            delai    = int(reponse.get("delai_max_approvisionnement_jours", 0)) if reponse else 0,
        )

        # Liens DemandeInfo → MatierePremiere
        for code in di.get("matieres_recherchees", []):
            tx.run("""
                MERGE (m:MatierePremiere {code: $code})
                ON CREATE SET m.note = 'Référencé depuis Achat'
            """, code=code)

            tx.run("""
                MATCH (d:DemandeInfo     {id:   $id_d})
                MATCH (m:MatierePremiere {code: $code})
                MERGE (d)-[:PORTE_SUR]->(m)
            """,
                id_d = di["num_demande"],
                code = code,
            )

        # Liens DemandeInfo → Fournisseurs consultés
        for id_f in reponse.get("fournisseurs_consultes", []):
            tx.run("""
                MERGE (f:Fournisseur {id: $id_f})
                ON CREATE SET f.note = 'Référencé depuis Achat'
            """, id_f=id_f)

            tx.run("""
                MATCH (d:DemandeInfo {id: $id_d})
                MATCH (f:Fournisseur {id: $id_f})
                MERGE (d)-[:CONSULTE]->(f)
            """,
                id_d = di["num_demande"],
                id_f = id_f,
            )

        print(f"  Demande info injectée : {di['num_demande']} ({di.get('service_demandeur')})")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M2 - Service Achat → Neo4j")
    print("=" * 50)

    print("\n[1/3] Fichier : achat_fournisseurs.json")
    with open(JSON_FOURNISSEURS, encoding="utf-8") as f:
        data_fournisseurs = json.load(f)
    with driver.session() as s:
        s.execute_write(inject_fournisseurs, data_fournisseurs)

    print("\n[2/3] Fichier : achat_bons_commande.json")
    with open(JSON_BONS_COMMANDE, encoding="utf-8") as f:
        data_bons = json.load(f)
    with driver.session() as s:
        s.execute_write(inject_bons_commande, data_bons)

    print("\n[3/3] Fichier : achat_demandes_info.json")
    with open(JSON_DEMANDES_INFO, encoding="utf-8") as f:
        data_demandes = json.load(f)
    with driver.session() as s:
        s.execute_write(inject_demandes_info, data_demandes)

    driver.close()

    print("\nInjection Achat terminée avec succès !")
    print("\nNœuds créés  : Fournisseur · BonCommande · DemandeInfo")
    print("Relations    : FOURNIT · ADRESSE_A · COMMANDE · DECLENCHE_PAR · PORTE_SUR · CONSULTE")
    print("Liens inter-services : BonCommande ─[DECLENCHE_PAR]─► Commande (M3 Vente)")
    print("                       Fournisseur ─[FOURNIT]─────────► MatierePremiere (M1 Stock)")
