import sys
import mysql.connector
from neo4j import GraphDatabase


# ══════════════════════════════════════════════════════════════
# CONFIGURATION — À ADAPTER À TON ENVIRONNEMENT
# ══════════════════════════════════════════════════════════════

MYSQL_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "123456",        # ← changer ici selon le mdp défini dans mysqlconnector
    "database": "navalcraft_stock",
}

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Password"        # ← changer ici selon le mdp défini dans neo4j


# ══════════════════════════════════════════════════════════════
# CONNEXIONS
# ══════════════════════════════════════════════════════════════

def connect():
    """Ouvre les connexions MySQL et Neo4j. Stoppe si échec."""
    try:
        db = mysql.connector.connect(**MYSQL_CONFIG)
        print("✓ MySQL connecté")
    except Exception as e:
        print(f"✗ Erreur MySQL : {e}")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        print("✓ Neo4j connecté")
    except Exception as e:
        print(f"✗ Erreur Neo4j : {e}")
        sys.exit(1)

    return db, driver


# ══════════════════════════════════════════════════════════════
# 1 — CATÉGORIES
#     Table MySQL : Categorie_Matiere
#     Nœud Neo4j  : (c:Categorie)
# ══════════════════════════════════════════════════════════════

def inject_categories(tx, rows):
    """
    Chaque ligne de Categorie_Matiere devient un nœud :Categorie dans Neo4j.
    MERGE garantit qu'on ne crée pas de doublons si on relance le script.
    """
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
        print(f"    • Catégorie : {row['nom_categorie']}")


# ══════════════════════════════════════════════════════════════
# 2 — EMPLACEMENTS
#     Table MySQL : Emplacement_Stock
#     Nœud Neo4j  : (e:Emplacement)
# ══════════════════════════════════════════════════════════════

def inject_emplacements(tx, rows):
    """
    Chaque emplacement physique de l'entrepôt devient un nœud :Emplacement.
    """
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
        print(f"    • Emplacement : {row['zone']}-{row['allee']}-{row['rayon']}-{row['niveau']}")


# ══════════════════════════════════════════════════════════════
# 3 — MATIÈRES PREMIÈRES + RELATIONS
#     Table MySQL : Matiere_premiere
#     Nœud Neo4j  : (m:MatierePremiere)
#     Relations   : (m)-[:APPARTIENT_A]->(c:Categorie)
#                   (m)-[:STOCKEE_EN]->(e:Emplacement)
#
#     ⚠ code_matiere = CLÉ DE LIAISON INTER-SERVICES
#       Les autres domaines (Achat, Vente, Production) référencent
#       les matières par ce code. C'est le "pont" entre les bases.
# ══════════════════════════════════════════════════════════════

def inject_matieres(tx, rows):
    for row in rows:
        # Créer / mettre à jour le nœud MatierePremiere
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

        # Relation : MatierePremiere → Categorie
        # MATCH retrouve les nœuds existants, MERGE crée la relation si absente
        tx.run("""
            MATCH (m:MatierePremiere {code: $code})
            MATCH (c:Categorie       {id:   $id_cat})
            MERGE (m)-[:APPARTIENT_A]->(c)
        """,
            code   = row["code_matiere"],
            id_cat = row["id_categorie"],
        )

        # Relation : MatierePremiere → Emplacement (si défini)
        if row["id_emplacement"]:
            tx.run("""
                MATCH (m:MatierePremiere {code: $code})
                MATCH (e:Emplacement     {id:   $id_emp})
                MERGE (m)-[:STOCKEE_EN]->(e)
            """,
                code   = row["code_matiere"],
                id_emp = row["id_emplacement"],
            )

        print(f"    • Matière : {row['code_matiere']} — {row['nom_matiere']}")


# ══════════════════════════════════════════════════════════════
# 4 — MOUVEMENTS DE STOCK
#     Table MySQL : Mouvement_Stock
#     Nœud Neo4j  : (mv:MouvementStock)
#     Relations   : (mv)-[:CONCERNE]->(m:MatierePremiere)
# ══════════════════════════════════════════════════════════════

def inject_mouvements(tx, rows):
    """
    Les mouvements (ENTREE / SORTIE) tracent l'historique du stock.
    On joint avec Matiere_premiere en MySQL pour récupérer le code_matiere
    qui sert de clé dans Neo4j.
    """
    for row in rows:
        tx.run("""
            MERGE (mv:MouvementStock {id: $id})
            SET mv.type_mouvement      = $type,
                mv.quantite            = $quantite,
                mv.date_mouvement      = $date,
                mv.responsable         = $responsable,
                mv.num_bon_livraison   = $bon_livraison,
                mv.num_ordre_livraison = $ordre_livraison,
                mv.service             = 'Stock'
        """,
            id             = row["id_mouvement"],
            type           = row["type_mouvement"],
            quantite       = float(row["quantite"]),
            date           = str(row["date_mouvement"]),
            responsable    = row["responsable"] or "",
            bon_livraison  = row["num_bon_livraison"] or "",
            ordre_livraison= row["num_ordre_livraison"] or "",
        )

        # Relation : MouvementStock → MatierePremiere concernée
        tx.run("""
            MATCH (mv:MouvementStock {id:   $id_mv})
            MATCH (m:MatierePremiere {code: $code})
            MERGE (mv)-[:CONCERNE]->(m)
        """,
            id_mv = row["id_mouvement"],
            code  = row["code_matiere"],
        )

        print(f"    • Mouvement : {row['type_mouvement']} — {row['code_matiere']} ({row['quantite']})")


# ══════════════════════════════════════════════════════════════
# 5 — ALERTES DE STOCK
#     Table MySQL : Alerte_Stock
#     Nœud Neo4j  : (a:AlerteStock)
#     Relations   : (a)-[:ALERTE_SUR]->(m:MatierePremiere)
# ══════════════════════════════════════════════════════════════

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

        # Relation : AlerteStock → MatierePremiere concernée
        tx.run("""
            MATCH (a:AlerteStock     {id:   $id_alerte})
            MATCH (m:MatierePremiere {code: $code})
            MERGE (a)-[:ALERTE_SUR]->(m)
        """,
            id_alerte = row["id_alerte"],
            code      = row["code_matiere"],
        )

        print(f"    • Alerte : {row['type_alerte']} — {row['code_matiere']}")


# ══════════════════════════════════════════════════════════════
# PROGRAMME PRINCIPAL
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  Injection M1 — Service STOCK (MySQL) → Neo4j")
    print("=" * 60)

    db, driver = connect()
    cursor = db.cursor(dictionary=True)  # dictionary=True → résultats en dict

    # ── Étape 1 : Catégories ──────────────────────────────────
    print("\n[1/5] Injection des Catégories...")
    cursor.execute("SELECT * FROM Categorie_Matiere")
    with driver.session() as s:
        s.execute_write(inject_categories, cursor.fetchall())

    # ── Étape 2 : Emplacements ────────────────────────────────
    print("\n[2/5] Injection des Emplacements...")
    cursor.execute("SELECT * FROM Emplacement_Stock")
    with driver.session() as s:
        s.execute_write(inject_emplacements, cursor.fetchall())

    # ── Étape 3 : Matières premières ──────────────────────────
    # ⚠ APRÈS catégories et emplacements (les relations en ont besoin)
    print("\n[3/5] Injection des Matières premières...")
    cursor.execute("SELECT * FROM Matiere_premiere")
    with driver.session() as s:
        s.execute_write(inject_matieres, cursor.fetchall())

    # ── Étape 4 : Mouvements de stock ─────────────────────────
    # JOIN pour récupérer code_matiere (clé Neo4j) depuis id_matiere (clé SQL)
    print("\n[4/5] Injection des Mouvements de stock...")
    cursor.execute("""
        SELECT mv.*, mp.code_matiere
        FROM Mouvement_Stock mv
        JOIN Matiere_premiere mp ON mv.id_matiere = mp.id_matiere
    """)
    with driver.session() as s:
        s.execute_write(inject_mouvements, cursor.fetchall())

    # ── Étape 5 : Alertes ─────────────────────────────────────
    print("\n[5/5] Injection des Alertes de stock...")
    cursor.execute("""
        SELECT a.*, mp.code_matiere
        FROM Alerte_Stock a
        JOIN Matiere_premiere mp ON a.id_matiere = mp.id_matiere
    """)
    with driver.session() as s:
        s.execute_write(inject_alertes, cursor.fetchall())

    # ── Nettoyage ─────────────────────────────────────────────
    cursor.close()
    db.close()
    driver.close()

    print("\n" + "=" * 60)
    print("  ✓ Injection STOCK terminée avec succès !")
    print("=" * 60)
    print("""
Nœuds créés dans Neo4j :
  • :Categorie          (ex: Bois, Résine, Métal...)
  • :Emplacement        (ex: Zone 2B, Allée 01...)
  • :MatierePremiere    (ex: BOIS-001, RES-001...)  ← clé inter-services
  • :MouvementStock     (ENTREE / SORTIE)
  • :AlerteStock        (SEUIL_MIN...)

Relations créées :
  (MatierePremiere)-[:APPARTIENT_A]→(Categorie)
  (MatierePremiere)-[:STOCKEE_EN]→(Emplacement)
  (MouvementStock)-[:CONCERNE]→(MatierePremiere)
  (AlerteStock)-[:ALERTE_SUR]→(MatierePremiere)

Lien inter-services (utilisé par Achat, Vente, Production) :
  code_matiere sur les nœuds :MatierePremiere
    """)
