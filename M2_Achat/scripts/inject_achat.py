"""
Injection M2 - Service Achat → Neo4j
Responsable : AM
Lit les 3 fichiers JSON et injecte les nœuds Fournisseur,
BonCommande, DemandeInfo et leurs relations dans Neo4j.
Mode BATCH (UNWIND) — supporte les très grands volumes.
"""

import json
import os
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Password"

_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FOURNISSEURS  = os.path.join(_DIR, "..", "achat_fournisseurs.json")
JSON_BONS_COMMANDE = os.path.join(_DIR, "..", "achat_bons_commande.json")
JSON_DEMANDES_INFO = os.path.join(_DIR, "..", "achat_demandes_info.json")

BATCH = 500

# ─────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_indexes(session):
    for q in [
        "CREATE INDEX bc_id  IF NOT EXISTS FOR (n:BonCommande) ON (n.id)",
        "CREATE INDEX fou_id IF NOT EXISTS FOR (n:Fournisseur) ON (n.id)",
    ]:
        session.run(q)


# ══════════════════════════════════════════
# FICHIER 1 — achat_fournisseurs.json  (individuel, petite table)
# ══════════════════════════════════════════
def inject_fournisseurs(tx, data):
    for f in data:
        contact     = f.get("contact", {})
        performance = f.get("performance", {})

        tx.run("""
            MERGE (f:Fournisseur {id: $id})
            SET f.nom                     = $nom,
                f.telephone               = $telephone,
                f.email                   = $email,
                f.adresse                 = $adresse,
                f.delai_livraison_moyen_j = $delai,
                f.pays                    = $pays,
                f.service                 = 'Achat'
        """, id=f["id_fournisseur"], nom=f.get("nom", ""),
             telephone=contact.get("telephone", ""), email=contact.get("email", ""),
             adresse=contact.get("adresse", ""),
             delai=performance.get("delai_livraison_moyen_jours", 0),
             pays=performance.get("pays", ""))

        for mat in f.get("catalogue_matieres", []):
            tx.run("""
                MERGE (m:MatierePremiere {code: $code})
                ON CREATE SET m.note = 'Référencé depuis Achat'
            """, code=mat["ref_matiere"])
            tx.run("""
                MATCH (f:Fournisseur     {id:   $id_f})
                MATCH (m:MatierePremiere {code: $code})
                MERGE (f)-[r:FOURNIT]->(m)
                SET r.prix_unitaire = $prix, r.unite = $unite
            """, id_f=f["id_fournisseur"], code=mat["ref_matiere"],
                 prix=float(mat.get("prix_unitaire", 0)),
                 unite=mat.get("unite", ""))
            print(f"    ↳ Catalogue : {f['id_fournisseur']} fournit {mat['ref_matiere']}")

        print(f"  Fournisseur injecté : {f['id_fournisseur']} — {f.get('nom', '')}")


# ══════════════════════════════════════════
# FICHIER 2 — achat_bons_commande.json  (BATCH)
# ══════════════════════════════════════════
def inject_bons_commande(session, data):
    total = len(data)
    print(f"  {total} bons de commande à injecter...")

    for idx, batch in enumerate(chunks(data, BATCH)):
        bc_data = [{
            "id":          bc["num_bon_commande"],
            "date_cmd":    bc.get("date_commande", ""),
            "fournisseur": bc["id_fournisseur"],
            "statut":      bc.get("statut", ""),
            "ht":          float(bc.get("montant_total_ht", 0)),
            "ttc":         float(bc.get("montant_total_ttc", 0)),
            "date_prev":   bc.get("date_livraison_prevue", ""),
            "date_eff":    bc.get("date_livraison_effective") or "",
            "bon_liv":     bc.get("num_bon_livraison")       or "",
            "cmd_client":  bc.get("commande_client_associee", ""),
            "mat":  bc["details_articles"][0]["ref_matiere"]              if bc.get("details_articles") else "",
            "qte":  float(bc["details_articles"][0].get("quantite_commandee",   0)) if bc.get("details_articles") else 0.0,
            "prix": float(bc["details_articles"][0].get("prix_unitaire_achat",  0)) if bc.get("details_articles") else 0.0,
        } for bc in batch]

        def _write_bc(tx, d=bc_data):
            tx.run("""
                UNWIND $rows AS row
                MERGE (bc:BonCommande {id: row.id})
                SET bc.date_commande            = row.date_cmd,
                    bc.statut                   = row.statut,
                    bc.montant_total_ht         = row.ht,
                    bc.montant_total_ttc        = row.ttc,
                    bc.date_livraison_prevue    = row.date_prev,
                    bc.date_livraison_effective = row.date_eff,
                    bc.num_bon_livraison        = row.bon_liv,
                    bc.commande_client_associee = row.cmd_client,
                    bc.service                  = 'Achat'
                WITH bc, row
                MATCH (f:Fournisseur {id: row.fournisseur})
                MERGE (bc)-[:ADRESSE_A]->(f)
                WITH bc, row
                WHERE row.mat <> ''
                MERGE (m:MatierePremiere {code: row.mat})
                ON CREATE SET m.note = 'Référencé depuis Achat'
                MERGE (bc)-[r:COMMANDE]->(m)
                SET r.quantite_commandee  = row.qte,
                    r.prix_unitaire_achat = row.prix
            """, rows=d)

        session.execute_write(_write_bc)
        print(f"  [{min((idx+1)*BATCH, total):>6}/{total}]", end="\r", flush=True)

    print(f"  [{total:>6}/{total}] ✓")

    # Liens DECLENCHE_PAR pour commandes CMD- uniquement
    cmd_links = [{"bc": bc["num_bon_commande"], "cmd": bc["commande_client_associee"]}
                 for bc in data if bc.get("commande_client_associee", "").startswith("CMD-")]
    if cmd_links:
        total_l = len(cmd_links)
        for idx, batch in enumerate(chunks(cmd_links, BATCH)):
            def _write_rel(tx, d=batch):
                tx.run("""
                    UNWIND $rows AS row
                    MERGE (cmd:Commande {id: row.cmd})
                    ON CREATE SET cmd.note = 'Référencé depuis Achat'
                    WITH cmd, row
                    MATCH (bc:BonCommande {id: row.bc})
                    MERGE (bc)-[:DECLENCHE_PAR]->(cmd)
                """, rows=d)
            session.execute_write(_write_rel)
        print(f"  DECLENCHE_PAR : {total_l} liens créés")


# ══════════════════════════════════════════
# FICHIER 3 — achat_demandes_info.json  (individuel, petite table)
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
        """, id=di["num_demande"], service=di.get("service_demandeur", ""),
             date=di.get("date_demande", ""), objet=di.get("objet", ""),
             date_rep=reponse.get("date_reponse", ""),
             cout=float(reponse.get("cout_total_estime_ht", 0)) if reponse else 0.0,
             delai=int(reponse.get("delai_max_approvisionnement_jours", 0)) if reponse else 0)

        for code in di.get("matieres_recherchees", []):
            tx.run("""
                MERGE (m:MatierePremiere {code: $code})
                ON CREATE SET m.note = 'Référencé depuis Achat'
            """, code=code)
            tx.run("""
                MATCH (d:DemandeInfo {id: $id_d}) MATCH (m:MatierePremiere {code: $code})
                MERGE (d)-[:PORTE_SUR]->(m)
            """, id_d=di["num_demande"], code=code)

        for id_f in reponse.get("fournisseurs_consultes", []):
            tx.run("""
                MERGE (f:Fournisseur {id: $id_f})
                ON CREATE SET f.note = 'Référencé depuis Achat'
            """, id_f=id_f)
            tx.run("""
                MATCH (d:DemandeInfo {id: $id_d}) MATCH (f:Fournisseur {id: $id_f})
                MERGE (d)-[:CONSULTE]->(f)
            """, id_d=di["num_demande"], id_f=id_f)

        print(f"  Demande info injectée : {di['num_demande']} ({di.get('service_demandeur')})")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M2 - Service Achat → Neo4j")
    print("=" * 50)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database="navalcraft") as s:
        print("\n[0] Création des index Neo4j")
        create_indexes(s)

    print("\n[1/3] Fichier : achat_fournisseurs.json")
    with open(JSON_FOURNISSEURS, encoding="utf-8") as f:
        data_fournisseurs = json.load(f)
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_fournisseurs, data_fournisseurs)

    print("\n[2/3] Fichier : achat_bons_commande.json")
    with open(JSON_BONS_COMMANDE, encoding="utf-8") as f:
        data_bons = json.load(f)
    with driver.session(database="navalcraft") as s:
        inject_bons_commande(s, data_bons)

    print("\n[3/3] Fichier : achat_demandes_info.json")
    with open(JSON_DEMANDES_INFO, encoding="utf-8") as f:
        data_demandes = json.load(f)
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_demandes_info, data_demandes)

    driver.close()

    print("\nInjection Achat terminée avec succès !")
    print("\nNœuds créés  : Fournisseur · BonCommande · DemandeInfo")
    print("Relations    : FOURNIT · ADRESSE_A · COMMANDE · DECLENCHE_PAR · PORTE_SUR · CONSULTE")
    print("Liens inter-services : BonCommande ─[DECLENCHE_PAR]─► Commande (M3 Vente)")
    print("                       Fournisseur ─[FOURNIT]─────────► MatierePremiere (M1 Stock)")
