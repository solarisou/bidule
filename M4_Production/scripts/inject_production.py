"""
Injection M4 - Service Production → Neo4j
Responsable : SG
Lit les 2 fichiers CSV et injecte les nœuds OrdreFabrication,
Bateau et les relations dans Neo4j.
"""

import csv
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"   # à changer

CSV_ORDRES         = "../production_ordres.csv"
CSV_NOMENCLATURES  = "../production_nomenclatures.csv"

# ─────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ══════════════════════════════════════════
# FICHIER 1 — production_ordres.csv
# ══════════════════════════════════════════
def inject_ordres(tx, rows):
    for row in rows:
        # Nœud Bateau
        tx.run("""
            MERGE (b:Bateau {ref: $ref})
            SET b.service = 'Production'
        """, ref=row["ref_bateau"])

        # Nœud OrdreFabrication
        tx.run("""
            MERGE (of:OrdreFabrication {id: $id})
            SET of.date_creation              = $date_creation,
                of.date_debut_prevue          = $date_debut,
                of.date_fin_prevue            = $date_fin,
                of.temps_production_estime_h  = toFloat($tps_estime),
                of.temps_production_reel_h    = toFloat($tps_reel),
                of.statut                     = $statut,
                of.service                    = 'Production'
        """,
            id           = row["num_ordre"],
            date_creation= row["date_creation"],
            date_debut   = row["date_debut_prevue"],
            date_fin     = row["date_fin_prevue"],
            tps_estime   = row["temps_production_estime_h"],
            tps_reel     = row["temps_production_reel_h"],
            statut       = row["statut"],
        )

        # Lien OrdreFabrication → Bateau
        tx.run("""
            MATCH (of:OrdreFabrication {id:  $id_of})
            MATCH (b:Bateau            {ref: $ref_b})
            MERGE (of)-[:PRODUIT]->(b)
        """,
            id_of = row["num_ordre"],
            ref_b = row["ref_bateau"],
        )

        # Lien inter-service : OrdreFabrication → Commande (M3 Vente)
        # num_commande est la clé de liaison
        tx.run("""
            MERGE (cmd:Commande {id: $id_cmd})
            ON CREATE SET cmd.note = 'Référencé depuis Production'
        """, id_cmd=row["num_commande"])

        tx.run("""
            MATCH (cmd:Commande        {id: $id_cmd})
            MATCH (of:OrdreFabrication {id: $id_of})
            MERGE (cmd)-[:DECLENCHE]->(of)
        """,
            id_cmd = row["num_commande"],
            id_of  = row["num_ordre"],
        )

        print(f"  Ordre injecté : {row['num_ordre']} ({row['statut']}) → {row['ref_bateau']}")


# ══════════════════════════════════════════
# FICHIER 2 — production_nomenclatures.csv
# ══════════════════════════════════════════
def inject_nomenclatures(tx, rows):
    for row in rows:
        # Le nœud MatierePremiere existe déjà (inject_stock.py)
        tx.run("""
            MERGE (m:MatierePremiere {code: $code})
            ON CREATE SET m.note = 'Référencé depuis Production'
        """, code=row["ref_matiere"])

        # Lien inter-service : OrdreFabrication → MatierePremiere (M1 Stock)
        tx.run("""
            MATCH (of:OrdreFabrication {id:   $id_of})
            MATCH (m:MatierePremiere   {code: $code})
            MERGE (of)-[r:NECESSITE]->(m)
            SET r.quantite_prevue   = toFloat($qte_prevue),
                r.quantite_utilisee = toFloat($qte_utilisee),
                r.unite             = $unite
        """,
            id_of        = row["num_ordre"],
            code         = row["ref_matiere"],
            qte_prevue   = row["quantite_prevue"],
            qte_utilisee = row["quantite_utilisee"],
            unite        = row["unite"],
        )

        print(f"    ↳ Nomenclature : {row['num_ordre']} nécessite {row['ref_matiere']} ({row['quantite_prevue']} {row['unite']})")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M4 - Service Production → Neo4j")
    print("=" * 50)

    print("\n[1/2] Fichier : production_ordres.csv")
    with open(CSV_ORDRES, encoding="utf-8", newline="") as f:
        rows_ordres = list(csv.DictReader(f, delimiter=";"))
    with driver.session() as s:
        s.execute_write(inject_ordres, rows_ordres)

    print("\n[2/2] Fichier : production_nomenclatures.csv")
    with open(CSV_NOMENCLATURES, encoding="utf-8", newline="") as f:
        rows_nomenclatures = list(csv.DictReader(f, delimiter=";"))
    with driver.session() as s:
        s.execute_write(inject_nomenclatures, rows_nomenclatures)

    driver.close()

    print("\nInjection Production terminée avec succès !")
    print("\nNœuds créés  : OrdreFabrication · Bateau")
    print("Relations    : PRODUIT · NECESSITE")
    print("Liens inter-services : Commande ─[DECLENCHE]──► OrdreFabrication (M3 Vente)")
    print("                       OrdreFabrication ─[NECESSITE]─► MatierePremiere (M1 Stock)")
