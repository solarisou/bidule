# NavalCraft — Projet d'Interopérabilité S2

Démonstration d'interopérabilité entre 4 systèmes hétérogènes centralisés dans une base de données graphe Neo4j, avec un dashboard Spring Boot.

## Architecture

```
M1_Stock      (SQL / MariaDB)   → Matières premières, mouvements de stock
M2_Achat      (JSON)            → Fournisseurs, bons de commande
M3_Vente      (XML)             → Clients, commandes
M4_Production (CSV)             → Ordres de fabrication, nomenclatures
        ↓ scripts Python (inject_*.py)
        Neo4j  [base : navalcraft]
        ↓
M5_Dashboard  (Spring Boot / Thymeleaf)  →  http://localhost:8080
```

## Prérequis

| Outil | Version | Notes |
|-------|---------|-------|
| Java  | 17+     | `java -version` |
| Maven | 3.8+    | `mvn -version` |
| Neo4j Desktop | 1.5+ | Créer une base **navalcraft**, lancer sur bolt://localhost:7687 |
| MariaDB | 11.7 | Password root : `Password` |
| Python | 3.9+ | `pip install neo4j` |

### Configuration Neo4j attendue
- Base : `navalcraft`
- Utilisateur : `neo4j`
- Mot de passe : `Password`
- URL bolt : `bolt://127.0.0.1:7687`

## Lancement

### 1. Générer les données

```bash
python generate_data.py
```

Génère ~43 000 nœuds : 1 000 clients, 3 000 commandes, 2 000 ordres, 5 000 bons de commande, 30 000 mouvements de stock.

### 2. Charger la base MariaDB

```bash
# Windows (adapter le chemin si nécessaire)
"C:/Program Files/MariaDB 11.7/bin/mysql.exe" -u root -pPassword < M1_Stock/stock_database.sql
```

### 3. Injecter dans Neo4j

S'assurer que Neo4j est démarré, puis lancer les 4 scripts dans l'ordre :

```bash
cd M1_Stock/scripts
python inject_stock.py

cd ../../M2_Achat/scripts
python inject_achat.py

cd ../../M3_Vente/scripts
python inject_vente.py

cd ../../M4_Production/scripts
python inject_production.py
```

> Sur Windows, si les caractères accentués posent problème : `set PYTHONIOENCODING=utf-8`

### 4. Lancer le dashboard

```bash
cd M5_Dashboard
mvn spring-boot:run
```

Ouvrir **http://localhost:8080**

## Structure du projet

```
projetNaval/
├── generate_data.py          ← génère tous les fichiers de données
├── M1_Stock/
│   ├── stock_database.sql
│   └── scripts/inject_stock.py
├── M2_Achat/
│   ├── achat_bons_commande.json
│   ├── achat_fournisseurs.json
│   ├── achat_demandes_info.json
│   └── scripts/inject_achat.py
├── M3_Vente/
│   ├── vente_referentiel_clients.xml
│   ├── vente_suivi_operations.xml
│   └── scripts/inject_vente.py
├── M4_Production/
│   ├── production_ordres.csv
│   ├── production_nomenclatures.csv
│   └── scripts/inject_production.py
└── M5_Dashboard/             ← Spring Boot 3.3.5
    └── src/main/
        ├── java/com/navalcraft/dashboard/
        │   ├── controller/   (Dashboard, Service, Tracabilite)
        │   ├── service/      (Stats, ServiceData)
        │   └── dto/          (Kpi)
        └── resources/templates/
            ├── dashboard.html
            ├── stock.html
            ├── achat.html
            ├── vente.html
            ├── production.html
            └── tracabilite.html
```

## Volume de données

| Entité | Quantité |
|--------|----------|
| Clients | 1 000 |
| Commandes | 3 000 |
| Ordres de fabrication | 2 000 |
| Bateaux | 2 000 |
| Bons de commande | 5 000 |
| Mouvements de stock | 30 000 |
| Fournisseurs | 5 |
| Matières premières | 10 |
| **Total nœuds** | **~43 000** |
| **Total relations** | **~50 000+** |
