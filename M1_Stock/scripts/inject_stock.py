"""
Injection M1 - Service Stock → Neo4j
Responsable : SG
Lit la base MySQL et injecte les nœuds MatierePremiere,
Categorie, Emplacement, Fournisseur (partiel) et les
relations dans Neo4j.
Mode BATCH (UNWIND) — supporte les très grands volumes.
"""

import mysql.connector
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "123456",   # à changer
    "database": "navalcraft_stock",
}

NEO4J_URI      = "bolt://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Password"

BATCH = 500  # enregistrements par transaction Neo4j

# ─────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ── Index Neo4j ───────────────────────────
def create_indexes(session):
    for q in [
        "CREATE INDEX mv_id   IF NOT EXISTS FOR (n:MouvementStock)  ON (n.id)",
        "CREATE INDEX mp_code IF NOT EXISTS FOR (n:MatierePremiere) ON (n.code)",
        "CREATE INDEX cat_id  IF NOT EXISTS FOR (n:Categorie)       ON (n.id)",
        "CREATE INDEX emp_id  IF NOT EXISTS FOR (n:Emplacement)     ON (n.id)",
        "CREATE INDEX fou_id  IF NOT EXISTS FOR (n:Fournisseur)     ON (n.id)",
    ]:
        session.run(q)


# ══════════════════════════════════════════
# 1 — Catégories (petite table, individuel)
# ══════════════════════════════════════════
def inject_categories(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (c:Categorie {id: $id})
            SET c.nom = $nom, c.description = $description,
                c.seuil_alerte = $seuil, c.unite_mesure = $unite,
                c.service = 'Stock'
        """, id=row["id_categorie"], nom=row["nom_categorie"],
             description=row["description"] or "",
             seuil=float(row["seuil_alerte"]), unite=row["unite_mesure"])
        print(f"  Catégorie : {row['nom_categorie']}")


# ══════════════════════════════════════════
# 2 — Emplacements
# ══════════════════════════════════════════
def inject_emplacements(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (e:Emplacement {id: $id})
            SET e.zone = $zone, e.allee = $allee, e.rayon = $rayon,
                e.niveau = $niveau, e.capacite_max = $capacite,
                e.service = 'Stock'
        """, id=row["id_emplacement"], zone=row["zone"], allee=row["allee"],
             rayon=row["rayon"], niveau=row["niveau"],
             capacite=float(row["capacite_max"]) if row["capacite_max"] else 0.0)
        print(f"  Emplacement : {row['zone']}-{row['allee']}-{row['rayon']}-{row['niveau']}")


# ══════════════════════════════════════════
# 3 — Matières premières + relations
# ══════════════════════════════════════════
def inject_matieres(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (m:MatierePremiere {code: $code})
            SET m.nom = $nom, m.description = $description,
                m.quantite_actuelle = $qte_actuelle,
                m.quantite_min = $qte_min, m.quantite_max = $qte_max,
                m.prix_unitaire = $prix, m.delai_approvisionnement = $delai,
                m.service = 'Stock'
        """, code=row["code_matiere"], nom=row["nom_matiere"],
             description=row["description"] or "",
             qte_actuelle=float(row["quantite_actuelle"]),
             qte_min=float(row["quantite_min"]),
             qte_max=float(row["quantite_max"]) if row["quantite_max"] else 0.0,
             prix=float(row["prix_unitaire"]) if row["prix_unitaire"] else 0.0,
             delai=int(row["delai_approvisionnement"]) if row["delai_approvisionnement"] else 0)

        tx.run("""
            MATCH (m:MatierePremiere {code: $code})
            MATCH (c:Categorie {id: $id_cat})
            MERGE (m)-[:APPARTIENT_A]->(c)
        """, code=row["code_matiere"], id_cat=row["id_categorie"])

        if row["id_emplacement"]:
            tx.run("""
                MATCH (m:MatierePremiere {code: $code})
                MATCH (e:Emplacement {id: $id_emp})
                MERGE (m)-[:STOCKEE_EN]->(e)
            """, code=row["code_matiere"], id_emp=row["id_emplacement"])

        print(f"  Matière : {row['code_matiere']} — {row['nom_matiere']}")


# ══════════════════════════════════════════
# 4 — Mouvements de stock — BATCH UNWIND
# ══════════════════════════════════════════
def inject_mouvements(session, rows):
    total = len(rows)
    print(f"  {total} mouvements à injecter (par lots de {BATCH})...")

    for idx, batch in enumerate(chunks(rows, BATCH)):
        # ── Nœuds + relation CONCERNE ─────────────────────────────
        data_nodes = [{
            "id":          row["id_mouvement"],
            "type":        row["type_mouvement"],
            "quantite":    float(row["quantite"]),
            "date":        str(row["date_mouvement"]),
            "bon_liv":     row["num_bon_livraison"]   or "",
            "ord_liv":     row["num_ordre_livraison"] or "",
            "responsable": row["responsable"]         or "",
            "code":        row["code_matiere"],
        } for row in batch]

        def _write_nodes(tx, d=data_nodes):
            tx.run("""
                UNWIND $rows AS row
                MERGE (mv:MouvementStock {id: row.id})
                SET mv.type_mouvement      = row.type,
                    mv.quantite            = row.quantite,
                    mv.date_mouvement      = row.date,
                    mv.num_bon_livraison   = row.bon_liv,
                    mv.num_ordre_livraison = row.ord_liv,
                    mv.responsable         = row.responsable,
                    mv.service             = 'Stock'
                WITH mv, row
                MATCH (m:MatierePremiere {code: row.code})
                MERGE (mv)-[:CONCERNE]->(m)
            """, rows=d)

        session.execute_write(_write_nodes)

        # ── Relation LIVREE_PAR (entrées seulement) ──────────────
        data_livree = [{
            "id":  row["id_mouvement"],
            "fid": f"F{str(row['id_fournisseur']).zfill(3)}",
        } for row in batch if row["id_fournisseur"]]

        if data_livree:
            def _write_livree(tx, d=data_livree):
                tx.run("""
                    UNWIND $rows AS row
                    MATCH (mv:MouvementStock {id: row.id})
                    MATCH (f:Fournisseur     {id: row.fid})
                    MERGE (mv)-[:LIVREE_PAR]->(f)
                """, rows=d)
            session.execute_write(_write_livree)

        done = min((idx + 1) * BATCH, total)
        print(f"  [{done:>6}/{total}]", end="\r", flush=True)

    print(f"  [{total:>6}/{total}] ✓                    ")


# ══════════════════════════════════════════
# 5 — Alertes de stock
# ══════════════════════════════════════════
def inject_alertes(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (a:AlerteStock {id: $id})
            SET a.type_alerte = $type, a.date_alerte = $date,
                a.quantite_actuelle = $qte_actuelle,
                a.quantite_seuil = $qte_seuil, a.service = 'Stock'
        """, id=row["id_alerte"], type=row["type_alerte"],
             date=str(row["date_alerte"]),
             qte_actuelle=float(row["quantite_actuelle"]),
             qte_seuil=float(row["quantite_seuil"]))

        tx.run("""
            MATCH (a:AlerteStock     {id:   $id_alerte})
            MATCH (m:MatierePremiere {code: $code})
            MERGE (a)-[:ALERTE_SUR]->(m)
        """, id_alerte=row["id_alerte"], code=row["code_matiere"])

        print(f"  Alerte : {row['type_alerte']} — {row['code_matiere']}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M1 - Service Stock → Neo4j")
    print("=" * 50)

    db     = mysql.connector.connect(**MYSQL_CONFIG)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    cursor = db.cursor(dictionary=True)

    with driver.session(database="navalcraft") as s:
        print("\n[0/5] Création des index Neo4j")
        create_indexes(s)

    print("\n[1/5] Catégories")
    cursor.execute("SELECT * FROM Categorie_Matiere")
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_categories, cursor.fetchall())

    print("\n[2/5] Emplacements")
    cursor.execute("SELECT * FROM Emplacement_Stock")
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_emplacements, cursor.fetchall())

    print("\n[3/5] Matières premières")
    cursor.execute("SELECT * FROM Matiere_premiere")
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_matieres, cursor.fetchall())

    print("\n[4/5] Mouvements de stock (BATCH)")
    cursor.execute("""
        SELECT mv.*, mp.code_matiere
        FROM Mouvement_Stock mv
        JOIN Matiere_premiere mp ON mv.id_matiere = mp.id_matiere
    """)
    rows = cursor.fetchall()
    with driver.session(database="navalcraft") as s:
        inject_mouvements(s, rows)

    print("\n[5/5] Alertes de stock")
    cursor.execute("""
        SELECT a.*, mp.code_matiere
        FROM Alerte_Stock a
        JOIN Matiere_premiere mp ON a.id_matiere = mp.id_matiere
    """)
    with driver.session(database="navalcraft") as s:
        s.execute_write(inject_alertes, cursor.fetchall())

    cursor.close()
    db.close()
    driver.close()

    print("\nInjection Stock terminée avec succès !")
    print("\nNœuds créés  : MatierePremiere · Categorie · Emplacement · MouvementStock · AlerteStock")
    print("Relations    : APPARTIENT_A · STOCKEE_EN · CONCERNE · LIVREE_PAR · ALERTE_SUR")
    print("Liens inter-services : MouvementStock ─[LIVREE_PAR]─► Fournisseur (M2 Achat)")
