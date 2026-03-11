"""
Injection M4 - Service Production → Neo4j
Responsable : SG
Lit les 2 fichiers CSV et injecte les nœuds OrdreFabrication,
Bateau et les relations dans Neo4j.
Mode BATCH (UNWIND) — supporte les très grands volumes.
"""

import csv
import os
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Password"

_DIR              = os.path.dirname(os.path.abspath(__file__))
CSV_ORDRES        = os.path.join(_DIR, "..", "production_ordres.csv")
CSV_NOMENCLATURES = os.path.join(_DIR, "..", "production_nomenclatures.csv")

BATCH = 500

# ─────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_indexes(session):
    for q in [
        "CREATE INDEX of_id  IF NOT EXISTS FOR (n:OrdreFabrication) ON (n.id)",
        "CREATE INDEX bat_ref IF NOT EXISTS FOR (n:Bateau)           ON (n.ref)",
        "CREATE INDEX cmd_id  IF NOT EXISTS FOR (n:Commande)         ON (n.id)",
        "CREATE INDEX mp_code IF NOT EXISTS FOR (n:MatierePremiere)  ON (n.code)",
    ]:
        session.run(q)


# ══════════════════════════════════════════
# FICHIER 1 — production_ordres.csv  (BATCH)
# ══════════════════════════════════════════
def inject_ordres(session, rows):
    total = len(rows)
    print(f"  {total} ordres à injecter...")

    for idx, batch in enumerate(chunks(rows, BATCH)):
        data = [{
            "id":           row["num_ordre"],
            "cmd":          row["num_commande"],
            "bat":          row["ref_bateau"],
            "date_creation": row["date_creation"],
            "date_debut":   row["date_debut_prevue"],
            "date_fin":     row["date_fin_prevue"],
            "tps_estime":   float(row["temps_production_estime_h"]),
            "tps_reel":     float(row["temps_production_reel_h"]),
            "statut":       row["statut"],
        } for row in batch]

        def writer(tx, d=data):
            tx.run("""
                UNWIND $rows AS row
                MERGE (b:Bateau {ref: row.bat})
                SET b.service = 'Production'
                MERGE (of:OrdreFabrication {id: row.id})
                SET of.date_creation             = row.date_creation,
                    of.date_debut_prevue         = row.date_debut,
                    of.date_fin_prevue           = row.date_fin,
                    of.temps_production_estime_h = row.tps_estime,
                    of.temps_production_reel_h   = row.tps_reel,
                    of.statut                    = row.statut,
                    of.service                   = 'Production'
                MERGE (of)-[:PRODUIT]->(b)
                WITH of, row
                MERGE (cmd:Commande {id: row.cmd})
                ON CREATE SET cmd.note = 'Référencé depuis Production'
                MERGE (cmd)-[:DECLENCHE]->(of)
            """, rows=d)

        session.execute_write(writer)
        print(f"  [{min((idx+1)*BATCH, total):>6}/{total}]", end="\r", flush=True)

    print(f"  [{total:>6}/{total}] ✓")


# ══════════════════════════════════════════
# FICHIER 2 — production_nomenclatures.csv  (BATCH)
# ══════════════════════════════════════════
def inject_nomenclatures(session, rows):
    total = len(rows)
    print(f"  {total} nomenclatures à injecter...")

    for idx, batch in enumerate(chunks(rows, BATCH)):
        data = [{
            "of_id":        row["num_ordre"],
            "code":         row["ref_matiere"],
            "qte_prevue":   float(row["quantite_prevue"]),
            "qte_utilisee": float(row["quantite_utilisee"]),
            "unite":        row["unite"],
        } for row in batch]

        def writer(tx, d=data):
            tx.run("""
                UNWIND $rows AS row
                MERGE (m:MatierePremiere {code: row.code})
                ON CREATE SET m.note = 'Référencé depuis Production'
                WITH m, row
                MATCH (of:OrdreFabrication {id: row.of_id})
                MERGE (of)-[r:NECESSITE]->(m)
                SET r.quantite_prevue   = row.qte_prevue,
                    r.quantite_utilisee = row.qte_utilisee,
                    r.unite             = row.unite
            """, rows=d)

        session.execute_write(writer)
        print(f"  [{min((idx+1)*BATCH, total):>6}/{total}]", end="\r", flush=True)

    print(f"  [{total:>6}/{total}] ✓")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M4 - Service Production → Neo4j")
    print("=" * 50)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database="navalcraft") as s:
        print("\n[0] Création des index Neo4j")
        create_indexes(s)

    print("\n[1/2] Fichier : production_ordres.csv")
    with open(CSV_ORDRES, encoding="utf-8", newline="") as f:
        rows_ordres = list(csv.DictReader(f, delimiter=";"))
    with driver.session(database="navalcraft") as s:
        inject_ordres(s, rows_ordres)

    print("\n[2/2] Fichier : production_nomenclatures.csv")
    with open(CSV_NOMENCLATURES, encoding="utf-8", newline="") as f:
        rows_nomenclatures = list(csv.DictReader(f, delimiter=";"))
    with driver.session(database="navalcraft") as s:
        inject_nomenclatures(s, rows_nomenclatures)

    driver.close()

    print("\nInjection Production terminée avec succès !")
    print("\nNœuds créés  : OrdreFabrication · Bateau")
    print("Relations    : PRODUIT · NECESSITE")
    print("Liens inter-services : Commande ─[DECLENCHE]──► OrdreFabrication (M3 Vente)")
    print("                       OrdreFabrication ─[NECESSITE]─► MatierePremiere (M1 Stock)")
