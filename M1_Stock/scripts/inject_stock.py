"""
Injection M1 - Service Stock → Neo4j
Responsable : SG
Lit la base MySQL et injecte les nœuds MatierePremiere,
Categorie, Emplacement, Fournisseur (partiel) et les
relations dans Neo4j.
"""

import mysql.connector
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "password",   # à changer
    "database": "navalcraft_stock",
}

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"   # à changer

# ─────────────────────────────────────────
# CONNEXIONS
# ─────────────────────────────────────────
db     = mysql.connector.connect(**MYSQL_CONFIG)
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ══════════════════════════════════════════
# 1 — Catégories
# ══════════════════════════════════════════
def inject_categories(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (c:Categorie {id: $id})
            SET c.nom          = $nom,
                c.description  = $description,
                c.seuil_alerte = $seuil,
                c.unite_mesure = $unite,
                c.service      = 'Stock'
        """,
            id          = row["id_categorie"],
            nom         = row["nom_categorie"],
            description = row["description"] or "",
            seuil       = float(row["seuil_alerte"]),
            unite       = row["unite_mesure"],
        )
        print(f"  Catégorie injectée : {row['nom_categorie']}")


# ══════════════════════════════════════════
# 2 — Emplacements
# ══════════════════════════════════════════
def inject_emplacements(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (e:Emplacement {id: $id})
            SET e.zone         = $zone,
                e.allee        = $allee,
                e.rayon        = $rayon,
                e.niveau       = $niveau,
                e.capacite_max = $capacite,
                e.service      = 'Stock'
        """,
            id       = row["id_emplacement"],
            zone     = row["zone"],
            allee    = row["allee"],
            rayon    = row["rayon"],
            niveau   = row["niveau"],
            capacite = float(row["capacite_max"]) if row["capacite_max"] else 0.0,
        )
        print(f"  Emplacement injecté : {row['zone']}-{row['allee']}-{row['rayon']}-{row['niveau']}")


# ══════════════════════════════════════════
# 3 — Matières premières + relations
# ══════════════════════════════════════════
def inject_matieres(tx, rows):
    for row in rows:
        # Nœud MatierePremiere (clé de liaison inter-services = code_matiere)
        tx.run("""
            MERGE (m:MatierePremiere {code: $code})
            SET m.nom                     = $nom,
                m.description             = $description,
                m.quantite_actuelle       = $qte_actuelle,
                m.quantite_min            = $qte_min,
                m.quantite_max            = $qte_max,
                m.prix_unitaire           = $prix,
                m.delai_approvisionnement = $delai,
                m.service                 = 'Stock'
        """,
            code         = row["code_matiere"],
            nom          = row["nom_matiere"],
            description  = row["description"] or "",
            qte_actuelle = float(row["quantite_actuelle"]),
            qte_min      = float(row["quantite_min"]),
            qte_max      = float(row["quantite_max"]) if row["quantite_max"] else 0.0,
            prix         = float(row["prix_unitaire"]) if row["prix_unitaire"] else 0.0,
            delai        = int(row["delai_approvisionnement"]) if row["delai_approvisionnement"] else 0,
        )

        # Lien MatierePremiere → Categorie
        tx.run("""
            MATCH (m:MatierePremiere {code: $code})
            MATCH (c:Categorie       {id:   $id_cat})
            MERGE (m)-[:APPARTIENT_A]->(c)
        """,
            code   = row["code_matiere"],
            id_cat = row["id_categorie"],
        )

        # Lien MatierePremiere → Emplacement
        if row["id_emplacement"]:
            tx.run("""
                MATCH (m:MatierePremiere {code: $code})
                MATCH (e:Emplacement     {id:   $id_emp})
                MERGE (m)-[:STOCKEE_EN]->(e)
            """,
                code   = row["code_matiere"],
                id_emp = row["id_emplacement"],
            )

        print(f"  Matière injectée : {row['code_matiere']} — {row['nom_matiere']}")


# ══════════════════════════════════════════
# 4 — Mouvements de stock
# ══════════════════════════════════════════
def inject_mouvements(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (mv:MouvementStock {id: $id})
            SET mv.type_mouvement      = $type,
                mv.quantite            = $quantite,
                mv.date_mouvement      = $date,
                mv.num_bon_livraison   = $bon_liv,
                mv.num_ordre_livraison = $ord_liv,
                mv.responsable         = $responsable,
                mv.service             = 'Stock'
        """,
            id          = row["id_mouvement"],
            type        = row["type_mouvement"],
            quantite    = float(row["quantite"]),
            date        = str(row["date_mouvement"]),
            bon_liv     = row["num_bon_livraison"]   or "",
            ord_liv     = row["num_ordre_livraison"] or "",
            responsable = row["responsable"]         or "",
        )

        # Lien MouvementStock → MatierePremiere
        tx.run("""
            MATCH (mv:MouvementStock  {id:   $id_mv})
            MATCH (m:MatierePremiere  {code: $code})
            MERGE (mv)-[:CONCERNE]->(m)
        """,
            id_mv = row["id_mouvement"],
            code  = row["code_matiere"],
        )

        # Lien inter-service : MouvementStock → Fournisseur (M2 Achat)
        # Le nœud Fournisseur sera complété par inject_achat.py
        if row["id_fournisseur"]:
            fournisseur_id = f"F{str(row['id_fournisseur']).zfill(3)}"
            tx.run("""
                MERGE (f:Fournisseur {id: $id_f})
                ON CREATE SET f.note = 'Référencé depuis Stock'
            """, id_f=fournisseur_id)

            tx.run("""
                MATCH (mv:MouvementStock {id:   $id_mv})
                MATCH (f:Fournisseur     {id:   $id_f})
                MERGE (mv)-[:LIVREE_PAR]->(f)
            """,
                id_mv = row["id_mouvement"],
                id_f  = fournisseur_id,
            )

        print(f"  Mouvement injecté : {row['type_mouvement']} — {row['code_matiere']} ({row['quantite']})")


# ══════════════════════════════════════════
# 5 — Alertes de stock
# ══════════════════════════════════════════
def inject_alertes(tx, rows):
    for row in rows:
        tx.run("""
            MERGE (a:AlerteStock {id: $id})
            SET a.type_alerte       = $type,
                a.date_alerte       = $date,
                a.quantite_actuelle = $qte_actuelle,
                a.quantite_seuil    = $qte_seuil,
                a.service           = 'Stock'
        """,
            id           = row["id_alerte"],
            type         = row["type_alerte"],
            date         = str(row["date_alerte"]),
            qte_actuelle = float(row["quantite_actuelle"]),
            qte_seuil    = float(row["quantite_seuil"]),
        )

        tx.run("""
            MATCH (a:AlerteStock     {id:   $id_alerte})
            MATCH (m:MatierePremiere {code: $code})
            MERGE (a)-[:ALERTE_SUR]->(m)
        """,
            id_alerte = row["id_alerte"],
            code      = row["code_matiere"],
        )

        print(f"  Alerte injectée : {row['type_alerte']} — {row['code_matiere']}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Injection M1 - Service Stock → Neo4j")
    print("=" * 50)

    cursor = db.cursor(dictionary=True)

    print("\n[1/5] Catégories")
    cursor.execute("SELECT * FROM Categorie_Matiere")
    with driver.session() as s:
        s.execute_write(inject_categories, cursor.fetchall())

    print("\n[2/5] Emplacements")
    cursor.execute("SELECT * FROM Emplacement_Stock")
    with driver.session() as s:
        s.execute_write(inject_emplacements, cursor.fetchall())

    print("\n[3/5] Matières premières")
    cursor.execute("SELECT * FROM Matiere_premiere")
    with driver.session() as s:
        s.execute_write(inject_matieres, cursor.fetchall())

    print("\n[4/5] Mouvements de stock")
    cursor.execute("""
        SELECT mv.*, mp.code_matiere
        FROM Mouvement_Stock mv
        JOIN Matiere_premiere mp ON mv.id_matiere = mp.id_matiere
    """)
    with driver.session() as s:
        s.execute_write(inject_mouvements, cursor.fetchall())

    print("\n[5/5] Alertes de stock")
    cursor.execute("""
        SELECT a.*, mp.code_matiere
        FROM Alerte_Stock a
        JOIN Matiere_premiere mp ON a.id_matiere = mp.id_matiere
    """)
    with driver.session() as s:
        s.execute_write(inject_alertes, cursor.fetchall())

    cursor.close()
    db.close()
    driver.close()

    print("\nInjection Stock terminée avec succès !")
    print("\nNœuds créés  : MatierePremiere · Categorie · Emplacement · MouvementStock · AlerteStock")
    print("Relations    : APPARTIENT_A · STOCKEE_EN · CONCERNE · LIVREE_PAR · ALERTE_SUR")
    print("Liens inter-services : MouvementStock ─[LIVREE_PAR]─► Fournisseur (M2 Achat)")
