"""
Générateur de données massives pour NavalCraft.
Cible : ~50 000 nœuds + ~60 000 relations dans Neo4j.
"""
import json, random, os
from datetime import date, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))

# ── Matières ────────────────────────────────────────────────────────────────
MATIERES = [
    ("BOIS-001", "Chene",               1, 1, 450.0,  1),
    ("BOIS-002", "Contreplaque marine", 1, 5, 180.0,  1),
    ("RES-001",  "Resine epoxy",        2, 2,  25.0,  2),
    ("ALU-001",  "Aluminium 5083",      3, 3, 120.0,  3),
    ("PEI-001",  "Antifouling marine",  4, 4,  35.0,  3),
    ("VIS-001",  "Visserie inox",       5, 6,   0.8,  4),
    ("ETR-001",  "Profile acier",       3, 7,  95.0,  3),
    ("FIL-001",  "Filin acier",         5, 8,  42.0,  4),
    ("CAR-001",  "Tissu fibre carbone", 6, 5, 320.0,  5),
    ("MAS-001",  "Mastic etancheite",   2, 6,  15.0,  2),
]
MAT_IDS = {m[0]: i + 1 for i, m in enumerate(MATIERES)}
FOURNISSEURS_PAR_MAT = {
    "BOIS-001": 1, "BOIS-002": 1,
    "RES-001":  2, "MAS-001":  2,
    "ALU-001":  3, "PEI-001":  3, "ETR-001": 3,
    "VIS-001":  4, "FIL-001":  4,
    "CAR-001":  5,
}
MAT_CODES = [m[0] for m in MATIERES]

# ── Noms pour 1000 clients (50 × 20 = 1000) ─────────────────────────────────
NOMS_FAMILLE = [
    "MARTIN",    "BERNARD",   "THOMAS",   "PETIT",    "ROBERT",
    "RICHARD",   "DURAND",    "DUBOIS",   "MOREAU",   "LAURENT",
    "SIMON",     "MICHEL",    "LEFEBVRE", "LEROY",    "ROUX",
    "DAVID",     "BERTRAND",  "MOREL",    "FOURNIER", "GIRARD",
    "BONNET",    "DUPONT",    "LAMBERT",  "FONTAINE", "ROUSSEAU",
    "VINCENT",   "MULLER",    "LECOMTE",  "FRANCOIS", "GARCIA",
    "PERROT",    "CLEMENT",   "GAUTHIER", "PERRIN",   "RENARD",
    "MORIN",     "MEYER",     "NICOLAS",  "LEGRAND",  "CHEVALIER",
    "BOYER",     "FAURE",     "ANDRE",    "MERCIER",  "BLANC",
    "GUERIN",    "NOEL",      "HENRY",    "MASSON",   "AUBERT",
]
PRENOMS = [
    "Jean",    "Marie",    "Pierre",    "Sophie",    "Laurent",
    "Anne",    "Luc",      "Claire",    "Philippe",  "Nathalie",
    "Michel",  "Sylvie",   "Antoine",   "Christine", "Bruno",
    "Helene",  "Denis",    "Cecile",    "Patrick",   "Isabelle",
]
VILLES = [
    ("13002", "Marseille",          "quai du Port"),
    ("44000", "Nantes",             "quai de la Fosse"),
    ("76600", "Le Havre",           "rue du Havre"),
    ("33000", "Bordeaux",           "cours du Medoc"),
    ("06000", "Nice",               "promenade des Anglais"),
    ("29200", "Brest",              "rue des Marins"),
    ("17000", "La Rochelle",        "quai Turenne"),
    ("35400", "Saint-Malo",         "rue des Corsaires"),
    ("83400", "Hyeres",             "impasse de la Voile"),
    ("56000", "Vannes",             "rue Saint-Nicolas"),
    ("50100", "Cherbourg",          "avenue de la Marine"),
    ("34200", "Sete",               "quai des Pecheurs"),
    ("64200", "Biarritz",           "avenue Victor Hugo"),
    ("22500", "Paimpol",            "rue du Port"),
    ("29300", "Quimper",            "impasse des Ancres"),
    ("66000", "Perpignan",          "boulevard Maritime"),
    ("85000", "La Roche-sur-Yon",   "allee des Voiliers"),
    ("56470", "La Trinite-sur-Mer", "rue de la Mer"),
    ("06400", "Cannes",             "avenue Victor Hugo"),
    ("42400", "Saint-Chamond",      "rue de l'universite"),
]
DOMAINES = [
    "marinefrance.fr", "atlanticboat.fr", "nautilyon.fr", "voilebordeaux.fr",
    "rivieraboat.fr",  "havrenaute.fr",   "meditboat.fr", "breizboats.fr",
    "cannesyachting.fr","loireyachts.fr", "varboats.fr",  "charenteroyale.fr",
    "morbihanyachts.fr","mancheboats.fr", "setenaute.fr", "pacanauts.fr",
    "cotesdarmor.fr",  "savoienauts.fr",  "malouins.fr",  "catalanboats.fr",
]

# ── Données fixes existantes (à préserver) ───────────────────────────────────
CLIENTS_FIXES_30 = [
    ("C001","GHERRAS",   "Salman Isfahani","24 rue de l'universite, 42400 Saint-Chamond",  "monEmail@email.com",              "07000000"),
    ("C002","MARTIN",    "Jean-Pierre",    "15 quai du Port, 13002 Marseille",               "jp.martin@marinefrance.fr",       "0491234567"),
    ("C003","DUPONT",    "Claire",         "8 rue de la Loire, 44000 Nantes",                "claire.dupont@atlanticboat.fr",   "0240987654"),
    ("C004","BERNARD",   "Michel",         "3 impasse des Goelettes, 69007 Lyon",            "m.bernard@nautilyon.fr",          "0478561234"),
    ("C005","MOREAU",    "Sophie",         "47 cours du Medoc, 33300 Bordeaux",              "sophie.moreau@voilebordeaux.fr",  "0556781234"),
    ("C006","LECLERC",   "Antoine",        "12 promenade des Anglais, 06000 Nice",           "a.leclerc@rivieraboat.fr",        "0493456789"),
    ("C007","ROUSSEAU",  "Philippe",       "5 rue du Havre, 76600 Le Havre",                 "p.rousseau@havrenaute.fr",        "0235456789"),
    ("C008","GARCIA",    "Marie",          "18 avenue de la Mer, 34500 Beziers",             "m.garcia@meditboat.fr",           "0467891234"),
    ("C009","PETIT",     "Luc",            "2 rue des Marins, 29200 Brest",                  "l.petit@breizboats.fr",           "0298123456"),
    ("C010","ROUX",      "Anne",           "33 avenue Victor Hugo, 06400 Cannes",            "a.roux@cannesyachting.fr",        "0493123456"),
    ("C011","LAMBERT",   "Thomas",         "7 quai de la Fosse, 44000 Nantes",               "t.lambert@loireyachts.fr",        "0240123456"),
    ("C012","SIMON",     "Isabelle",       "22 rue du Commerce, 33000 Bordeaux",             "i.simon@garonneyachts.fr",        "0556123456"),
    ("C013","MICHEL",    "Frederic",       "9 impasse de la Voile, 83400 Hyeres",            "f.michel@varboats.fr",            "0494123456"),
    ("C014","LEROY",     "Christine",      "14 boulevard de la Plage, 17000 La Rochelle",    "c.leroy@charenteroyale.fr",       "0546123456"),
    ("C015","FOURNIER",  "Patrick",        "6 rue Saint-Nicolas, 56000 Vannes",              "p.fournier@morbihanyachts.fr",    "0297123456"),
    ("C016","GIRARD",    "Nathalie",       "31 avenue de la Marine, 50100 Cherbourg",        "n.girard@mancheboats.fr",         "0233123456"),
    ("C017","VINCENT",   "Laurent",        "11 quai des Pecheurs, 34200 Sete",               "l.vincent@setenaute.fr",          "0467234567"),
    ("C018","MARTINEZ",  "Sylvie",         "3 rue de la Sardine, 13008 Marseille",           "s.martinez@pacanauts.fr",         "0491345678"),
    ("C019","NICOLAS",   "Bruno",          "19 rue du Port, 22500 Paimpol",                  "b.nicolas@cotesdarmor.fr",        "0296123456"),
    ("C020","FONTAINE",  "Helene",         "25 avenue du Lac, 73100 Aix-les-Bains",          "h.fontaine@savoienauts.fr",       "0479123456"),
    ("C021","THOMAS",    "Denis",          "8 rue des Corsaires, 35400 Saint-Malo",          "d.thomas@malouins.fr",            "0299123456"),
    ("C022","DUMONT",    "Cecile",         "42 boulevard Maritime, 66000 Perpignan",         "c.dumont@catalanboats.fr",        "0468123456"),
    ("C023","BONNET",    "Alain",          "16 rue du Vieux Port, 13002 Marseille",          "a.bonnet@phocaboats.fr",          "0491456789"),
    ("C024","FRANCOIS",  "Monique",        "5 place du Port, 30220 Aigues-Mortes",           "m.francois@camargueyachts.fr",    "0466123456"),
    ("C025","GARNIER",   "Pierre",         "29 quai Turenne, 17000 La Rochelle",             "p.garnier@rochellaboats.fr",      "0546234567"),
    ("C026","CHEVALIER", "Veronique",      "7 impasse des Ancres, 29300 Quimper",            "v.chevalier@finistereboats.fr",   "0298234567"),
    ("C027","CLEMENT",   "Olivier",        "13 avenue Victor Hugo, 64200 Biarritz",          "o.clement@basquenauts.fr",        "0559123456"),
    ("C028","RICHARD",   "Stephanie",      "20 rue de la Plage, 14470 Courseulles-sur-Mer",  "s.richard@normandieboats.fr",     "0231123456"),
    ("C029","HENRY",     "Christian",      "4 allee des Voiliers, 85000 La Roche-sur-Yon",  "c.henry@vendeeocean.fr",          "0251123456"),
    ("C030","PERROT",    "Francoise",      "38 rue de la Mer, 56470 La Trinite-sur-Mer",     "f.perrot@morbihansailing.fr",     "0297234567"),
]

COMMANDES_FIXES = [
    ("C001","CMD-2026-001","2026-02-11","2026-04-15","EN_COURS",  "45000.00"),
    ("C002","C-2026-015",  "2026-01-10","2026-03-20","EN_COURS",  "38000.00"),
    ("C003","C-2026-010",  "2025-10-15","2026-01-15","LIVREE",    "52000.00"),
    ("C004","CMD-2026-002","2026-02-18","2026-05-30","PLANIFIEE", "67000.00"),
    ("C005","CMD-2026-003","2026-03-01","2026-05-01","EN_COURS",  "31000.00"),
    ("C006","CMD-2026-004","2025-09-10","2026-01-20","LIVREE",    "89000.00"),
]

ORDRES_FIXES = [
    ("OF-2026-001","CMD-2026-001","BAT-2026-001","2026-02-02","2026-02-10","2026-04-10",320,  0,  "PLANIFIE"),
    ("OF-2026-002","C-2026-015",  "BAT-2026-002","2026-01-15","2026-01-20","2026-03-20",280,145, "EN_COURS"),
    ("OF-2026-003","C-2026-010",  "BAT-2026-003","2025-11-01","2025-11-10","2026-01-10",350,350, "TERMINE"),
    ("OF-2026-004","CMD-2026-002","BAT-2026-004","2026-02-20","2026-03-01","2026-05-20",400,  0,  "PLANIFIE"),
    ("OF-2026-005","CMD-2026-003","BAT-2026-005","2026-03-03","2026-03-10","2026-04-25",180, 40, "EN_COURS"),
    ("OF-2026-006","CMD-2026-004","BAT-2026-006","2025-09-15","2025-09-25","2026-01-15",520,520, "TERMINE"),
]
NOMEN_FIXES = [
    ("OF-2026-001","BOIS-001",2.5, 0,   "m3"),  ("OF-2026-001","RES-001",10.0,0,   "L"),
    ("OF-2026-002","BOIS-001",2.0, 2.0, "m3"),  ("OF-2026-002","RES-001",8.0, 4.0, "L"),   ("OF-2026-002","ALU-001",15.0,10.0,"m2"),
    ("OF-2026-003","BOIS-001",3.0, 3.0, "m3"),  ("OF-2026-003","RES-001",12.0,12.0,"L"),   ("OF-2026-003","PEI-001",20.0,20.0,"L"),
    ("OF-2026-004","BOIS-002",8.0, 0,   "m2"),  ("OF-2026-004","ALU-001",25.0,0,   "m2"),  ("OF-2026-004","VIS-001",200.0,0,"piece"),("OF-2026-004","CAR-001",6.0,0,"m2"),
    ("OF-2026-005","CAR-001", 4.0, 4.0, "m2"),  ("OF-2026-005","VIS-001",100.0,40.0,"piece"),("OF-2026-005","FIL-001",5.0,2.0,"bobine"),("OF-2026-005","MAS-001",15.0,6.0,"L"),
    ("OF-2026-006","BOIS-001",5.0, 5.0, "m3"),  ("OF-2026-006","ALU-001",40.0,40.0,"m2"),  ("OF-2026-006","VIS-001",400.0,400.0,"piece"),("OF-2026-006","ETR-001",20.0,20.0,"m"),("OF-2026-006","PEI-001",30.0,30.0,"L"),
]

BCS_FIXES = [
    {"num_bon_commande":"BC-2026-001","date_commande":"2026-01-20","id_fournisseur":"F001","statut":"LIVREE","details_articles":[{"ref_matiere":"BOIS-001","quantite_commandee":15.0,"prix_unitaire_achat":450.00}],"montant_total_ht":6750.00,"montant_total_ttc":8100.00,"date_livraison_prevue":"2026-02-03","date_livraison_effective":"2026-02-02","num_bon_livraison":"ENT-2026-001","commande_client_associee":"C-2026-015"},
    {"num_bon_commande":"BC-2026-002","date_commande":"2026-01-25","id_fournisseur":"F002","statut":"LIVREE","details_articles":[{"ref_matiere":"RES-001","quantite_commandee":300.0,"prix_unitaire_achat":25.00}],"montant_total_ht":7500.00,"montant_total_ttc":9000.00,"date_livraison_prevue":"2026-02-05","date_livraison_effective":"2026-02-04","num_bon_livraison":"ENT-2026-002","commande_client_associee":"CMD-2026-001"},
    {"num_bon_commande":"BC-2026-003","date_commande":"2026-02-20","id_fournisseur":"F001","statut":"EN_COURS","details_articles":[{"ref_matiere":"BOIS-001","quantite_commandee":20.0,"prix_unitaire_achat":450.00}],"montant_total_ht":9000.00,"montant_total_ttc":10800.00,"date_livraison_prevue":"2026-03-10","date_livraison_effective":None,"num_bon_livraison":None,"commande_client_associee":"C-2026-022"},
    {"num_bon_commande":"BC-2026-004","date_commande":"2026-01-14","id_fournisseur":"F001","statut":"LIVREE","details_articles":[{"ref_matiere":"BOIS-002","quantite_commandee":30.0,"prix_unitaire_achat":180.00}],"montant_total_ht":5400.00,"montant_total_ttc":6480.00,"date_livraison_prevue":"2026-01-28","date_livraison_effective":"2026-01-26","num_bon_livraison":"ENT-2026-003","commande_client_associee":"CMD-2026-001"},
    {"num_bon_commande":"BC-2026-005","date_commande":"2026-01-20","id_fournisseur":"F003","statut":"LIVREE","details_articles":[{"ref_matiere":"ETR-001","quantite_commandee":80.0,"prix_unitaire_achat":95.00}],"montant_total_ht":7600.00,"montant_total_ttc":9120.00,"date_livraison_prevue":"2026-02-10","date_livraison_effective":"2026-02-08","num_bon_livraison":"ENT-2026-005","commande_client_associee":"C-2026-015"},
    {"num_bon_commande":"BC-2026-006","date_commande":"2026-01-17","id_fournisseur":"F004","statut":"LIVREE","details_articles":[{"ref_matiere":"VIS-001","quantite_commandee":600.0,"prix_unitaire_achat":0.80}],"montant_total_ht":480.00,"montant_total_ttc":576.00,"date_livraison_prevue":"2026-01-25","date_livraison_effective":"2026-01-23","num_bon_livraison":"ENT-2026-004","commande_client_associee":"C-2026-015"},
    {"num_bon_commande":"BC-2026-007","date_commande":"2026-01-28","id_fournisseur":"F005","statut":"LIVREE","details_articles":[{"ref_matiere":"CAR-001","quantite_commandee":22.0,"prix_unitaire_achat":320.00}],"montant_total_ht":7040.00,"montant_total_ttc":8448.00,"date_livraison_prevue":"2026-02-20","date_livraison_effective":"2026-02-18","num_bon_livraison":"ENT-2026-006","commande_client_associee":"C-2026-010"},
    {"num_bon_commande":"BC-2026-008","date_commande":"2026-01-29","id_fournisseur":"F002","statut":"LIVREE","details_articles":[{"ref_matiere":"MAS-001","quantite_commandee":150.0,"prix_unitaire_achat":15.00}],"montant_total_ht":2250.00,"montant_total_ttc":2700.00,"date_livraison_prevue":"2026-02-10","date_livraison_effective":"2026-02-09","num_bon_livraison":"ENT-2026-007","commande_client_associee":"C-2026-010"},
    {"num_bon_commande":"BC-2026-009","date_commande":"2026-02-25","id_fournisseur":"F003","statut":"EN_COURS","details_articles":[{"ref_matiere":"ALU-001","quantite_commandee":30.0,"prix_unitaire_achat":120.00}],"montant_total_ht":3600.00,"montant_total_ttc":4320.00,"date_livraison_prevue":"2026-03-20","date_livraison_effective":None,"num_bon_livraison":None,"commande_client_associee":"CMD-2026-002"},
    {"num_bon_commande":"BC-2026-010","date_commande":"2026-03-01","id_fournisseur":"F004","statut":"EN_COURS","details_articles":[{"ref_matiere":"FIL-001","quantite_commandee":15.0,"prix_unitaire_achat":42.00}],"montant_total_ht":630.00,"montant_total_ttc":756.00,"date_livraison_prevue":"2026-03-08","date_livraison_effective":None,"num_bon_livraison":None,"commande_client_associee":"CMD-2026-003"},
]

FOURNISSEUR_MATS = {
    "F001": [("BOIS-001", 450.0), ("BOIS-002", 180.0)],
    "F002": [("RES-001",  25.0),  ("MAS-001",  15.0)],
    "F003": [("ALU-001",  120.0), ("PEI-001",  35.0), ("ETR-001", 95.0)],
    "F004": [("VIS-001",  0.8),   ("FIL-001",  42.0)],
    "F005": [("CAR-001",  320.0), ("MAS-001",  15.0)],
}

TYPES_BATEAUX = [
    "Voilier Croisiere","Vedette Express","Catamaran","Pneumatique Rigide",
    "Yacht Moteur","Coque Open Aluminium","Vedette Cabinee","Semi-Rigide",
    "Dayboat","Runabout","Fishing Boat","Trawler",
]
MATS_PAR_TYPE = {
    "Voilier Croisiere":    [("BOIS-001","m3",2.0,4.0),   ("RES-001","L",8.0,15.0),   ("VIS-001","piece",150.0,300.0), ("PEI-001","L",15.0,25.0)],
    "Vedette Express":      [("ALU-001","m2",15.0,30.0),  ("VIS-001","piece",200.0,400.0),("ETR-001","m",10.0,20.0),  ("PEI-001","L",10.0,20.0)],
    "Catamaran":            [("BOIS-002","m2",6.0,10.0),  ("ALU-001","m2",20.0,35.0),  ("CAR-001","m2",4.0,8.0),     ("VIS-001","piece",300.0,500.0)],
    "Pneumatique Rigide":   [("CAR-001","m2",3.0,6.0),   ("VIS-001","piece",80.0,150.0),("FIL-001","bobine",3.0,6.0),("MAS-001","L",10.0,20.0)],
    "Yacht Moteur":         [("BOIS-001","m3",4.0,7.0),  ("ALU-001","m2",30.0,50.0),  ("VIS-001","piece",350.0,600.0),("ETR-001","m",15.0,30.0),("PEI-001","L",25.0,40.0)],
    "Coque Open Aluminium": [("ALU-001","m2",12.0,22.0), ("VIS-001","piece",120.0,250.0),("ETR-001","m",8.0,16.0),   ("PEI-001","L",8.0,15.0)],
    "Vedette Cabinee":      [("BOIS-002","m2",4.0,8.0),  ("ALU-001","m2",10.0,20.0),  ("VIS-001","piece",150.0,280.0),("MAS-001","L",8.0,15.0)],
    "Semi-Rigide":          [("CAR-001","m2",2.0,4.0),   ("VIS-001","piece",60.0,120.0),("FIL-001","bobine",2.0,4.0)],
    "Dayboat":              [("BOIS-001","m3",1.5,3.0),  ("RES-001","L",6.0,12.0),    ("VIS-001","piece",80.0,160.0),("PEI-001","L",6.0,12.0)],
    "Runabout":             [("BOIS-002","m2",3.0,6.0),  ("RES-001","L",4.0,8.0),     ("VIS-001","piece",60.0,120.0)],
    "Fishing Boat":         [("ALU-001","m2",8.0,15.0),  ("VIS-001","piece",100.0,200.0),("FIL-001","bobine",2.0,5.0),("PEI-001","L",8.0,15.0)],
    "Trawler":              [("BOIS-001","m3",5.0,8.0),  ("ALU-001","m2",25.0,40.0),  ("VIS-001","piece",300.0,500.0),("ETR-001","m",20.0,35.0),("PEI-001","L",30.0,50.0)],
}

STATUTS_CMD = ["EN_COURS","EN_COURS","EN_COURS","PLANIFIEE","LIVREE","LIVREE"]
STATUTS_OF  = ["PLANIFIE","EN_COURS","EN_COURS","TERMINE"]

def rand_date(start="2021-01-01", end="2026-03-01"):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return s + timedelta(days=random.randint(0, (e - s).days))

def fmt(d): return str(d)

random.seed(42)

# ════════════════════════════════════════════════════════════════════
# 1 — CLIENTS (1 000)
# ════════════════════════════════════════════════════════════════════
# C001-C030 : données existantes préservées
# C031-C1000 : générés depuis NOMS_FAMILLE × PRENOMS
all_clients = list(CLIENTS_FIXES_30)
idx_extra = 0
for nom in NOMS_FAMILLE:
    for prenom in PRENOMS:
        n = len(all_clients) + 1
        if n > 1000:
            break
        cid = f"C{n:04d}"
        cp, ville, rue = random.choice(VILLES)
        num = random.randint(1, 99)
        dom = random.choice(DOMAINES)
        email = f"{prenom[0].lower()}.{nom.lower()}@{dom}"
        tel = f"0{random.randint(1,9)}{random.randint(10000000,99999999)}"
        adresse = f"{num} {rue}, {cp} {ville}"
        all_clients.append((cid, nom, prenom, adresse, email, tel))
    if len(all_clients) >= 1000:
        break

print(f"Clients générés : {len(all_clients)}")

# XML clients
lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<referentiel_clients>']
for cid, nom, prenom, adresse, email, tel in all_clients:
    lines.append(f'    <client id="{cid}">')
    lines.append(f'        <identite><nom>{nom}</nom><prenom>{prenom}</prenom></identite>')
    lines.append(f'        <contact><adresse>{adresse}</adresse><email>{email}</email><telephone>{tel}</telephone></contact>')
    lines.append( '    </client>')
lines.append('</referentiel_clients>')
with open(BASE + r"\M3_Vente\vente_referentiel_clients.xml", "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print(f"  → vente_referentiel_clients.xml ({len(all_clients)} clients)")

# ════════════════════════════════════════════════════════════════════
# 2 — COMMANDES (3 000)
# ════════════════════════════════════════════════════════════════════
# 6 commandes fixes + 2994 générées
ANNEES_CMD = [2023, 2023, 2024, 2024, 2024, 2025, 2025, 2025, 2026]
extra_cmds = []
for i in range(2994):
    n      = i + 7                  # CMD-20XX-0007 onwards
    annee  = random.choice(ANNEES_CMD)
    cmd_id = f"CMD-{annee}-{n:04d}"
    cid_n  = (i % 997) + 4         # C004-C1000 (keep C001-C003 for fixed orders)
    cid    = f"C{cid_n:04d}"
    d_cmd  = rand_date(f"{annee}-01-01", f"{annee}-12-31")
    d_liv  = d_cmd + timedelta(days=random.randint(45, 180))
    statut = random.choice(STATUTS_CMD)
    if d_liv < date(2026, 1, 1):
        statut = "LIVREE"
    montant = random.choice([18000,22000,28000,35000,42000,48000,55000,63000,70000,78000,
                              85000,92000,31000,44000,57000,66000,74000,81000,89000,95000])
    extra_cmds.append((cid, cmd_id, fmt(d_cmd), fmt(d_liv), statut, f"{montant}.00"))

ALL_CMDS = [r[1] for r in COMMANDES_FIXES] + [r[1] for r in extra_cmds]

# XML opérations : dossier C001 avec demande+devis, puis tous les autres
op_lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<suivi_operations>']
op_lines.append('    <dossier client_ref="C001">')
op_lines.append('        <demande id="DEM-2026-001"><date_reception>2026-02-01</date_reception><type_bateau>Coque Open Aluminium</type_bateau><statut>ACCEPTEE</statut></demande>')
op_lines.append('        <devis id="DEV-2026-001"><details_financiers ht="37500.00" ttc="45000.00" prix_revient="28500.00"/><logistique delai="60" validite="2026-03-01"/><estimation_matieres><matiere ref="BOIS-001" quantite="2.5" unite="m3" prix_unitaire="450.00"/><matiere ref="RES-001" quantite="10.0" unite="L" prix_unitaire="25.00"/></estimation_matieres></devis>')
op_lines.append('        <commande id="CMD-2026-001"><date_commande>2026-02-11</date_commande><livraison_prevue>2026-04-15</livraison_prevue><statut>EN_COURS</statut><reglement montant="45000.00"/></commande>')
op_lines.append('    </dossier>')

for cid, cmd_id, d_cmd, d_liv, statut, montant in COMMANDES_FIXES[1:]:
    op_lines.append(f'    <dossier client_ref="{cid}">')
    op_lines.append(f'        <commande id="{cmd_id}"><date_commande>{d_cmd}</date_commande><livraison_prevue>{d_liv}</livraison_prevue><statut>{statut}</statut><reglement montant="{montant}"/></commande>')
    op_lines.append( '    </dossier>')

for cid, cmd_id, d_cmd, d_liv, statut, montant in extra_cmds:
    op_lines.append(f'    <dossier client_ref="{cid}">')
    op_lines.append(f'        <commande id="{cmd_id}"><date_commande>{d_cmd}</date_commande><livraison_prevue>{d_liv}</livraison_prevue><statut>{statut}</statut><reglement montant="{montant}"/></commande>')
    op_lines.append( '    </dossier>')

op_lines.append('</suivi_operations>')
with open(BASE + r"\M3_Vente\vente_suivi_operations.xml", "w", encoding="utf-8") as f:
    f.write("\n".join(op_lines) + "\n")
total_cmds = 1 + len(COMMANDES_FIXES) - 1 + len(extra_cmds)
print(f"  → vente_suivi_operations.xml ({total_cmds} commandes)")

# ════════════════════════════════════════════════════════════════════
# 3 — ORDRES DE FABRICATION (2 000) + NOMENCLATURES (~8 000)
# ════════════════════════════════════════════════════════════════════
# 6 fixes + 1994 générés
cmds_pool = [r[1] for r in extra_cmds]  # 2994 commandes générées
extra_ordres = []
extra_nomen  = []

for i in range(1994):
    n      = 7 + i
    # Alterner années pour réalisme
    annee  = random.choice([2023, 2023, 2024, 2024, 2024, 2025, 2025, 2026])
    of_id  = f"OF-{annee}-{n:04d}"
    bat_id = f"BAT-{annee}-{n:04d}"
    cmd_id = cmds_pool[i % len(cmds_pool)]
    type_b = TYPES_BATEAUX[i % len(TYPES_BATEAUX)]
    d_cre  = rand_date("2022-01-01", "2026-02-01")
    d_deb  = d_cre + timedelta(days=random.randint(7, 30))
    tps_e  = random.randint(80, 600)
    statut = random.choice(STATUTS_OF)
    if d_deb + timedelta(days=tps_e // 8) < date(2025, 6, 1):
        statut = "TERMINE"
        tps_r  = tps_e
    elif statut == "EN_COURS":
        tps_r  = random.randint(10, tps_e - 5)
    else:
        tps_r  = 0
    d_fin  = d_deb + timedelta(days=tps_e // 8)

    extra_ordres.append((of_id, cmd_id, bat_id, fmt(d_cre), fmt(d_deb), fmt(d_fin), tps_e, tps_r, statut))

    mats = MATS_PAR_TYPE.get(type_b, MATS_PAR_TYPE["Vedette Express"])
    for mat_code, unite, qmin, qmax in mats:
        qte  = round(random.uniform(qmin, qmax), 1)
        util = round(qte * (tps_r / tps_e), 1) if tps_e > 0 and statut != "PLANIFIE" else 0.0
        extra_nomen.append((of_id, mat_code, qte, util, unite))

ord_lines = ["num_ordre;num_commande;ref_bateau;date_creation;date_debut_prevue;date_fin_prevue;temps_production_estime_h;temps_production_reel_h;statut"]
for r in ORDRES_FIXES:
    ord_lines.append(";".join(str(x) for x in r))
for r in extra_ordres:
    ord_lines.append(";".join(str(x) for x in r))
with open(BASE + r"\M4_Production\production_ordres.csv", "w", encoding="utf-8") as f:
    f.write("\n".join(ord_lines) + "\n")

nom_lines = ["num_ordre;ref_matiere;quantite_prevue;quantite_utilisee;unite"]
for r in NOMEN_FIXES:
    nom_lines.append(";".join(str(x) for x in r))
for r in extra_nomen:
    nom_lines.append(";".join(str(x) for x in r))
with open(BASE + r"\M4_Production\production_nomenclatures.csv", "w", encoding="utf-8") as f:
    f.write("\n".join(nom_lines) + "\n")

print(f"  → production_ordres.csv ({len(ORDRES_FIXES)+len(extra_ordres)} ordres)")
print(f"  → production_nomenclatures.csv ({len(NOMEN_FIXES)+len(extra_nomen)} nomenclatures)")

# ════════════════════════════════════════════════════════════════════
# 4 — BONS DE COMMANDE (5 000)
# ════════════════════════════════════════════════════════════════════
# 10 fixes + 4990 générés
extra_bcs = []
for i in range(4990):
    n      = 11 + i
    bc_id  = f"BC-{random.choice([2023,2024,2025,2026])}-{n:05d}"
    f_id   = random.choice(["F001","F002","F003","F004","F005"])
    mat, prix = random.choice(FOURNISSEUR_MATS[f_id])
    qte    = round(random.uniform(5, 300), 1)
    ht     = round(qte * prix, 2)
    ttc    = round(ht * 1.2, 2)
    d_cmd  = rand_date("2021-06-01", "2026-02-15")
    d_prev = d_cmd + timedelta(days=random.randint(7, 30))
    statut = random.choice(["LIVREE","LIVREE","LIVREE","EN_COURS"])
    d_eff  = fmt(d_prev - timedelta(days=random.randint(0, 3))) if statut == "LIVREE" else None
    bon_liv = f"ENT-{n:06d}" if statut == "LIVREE" else None
    cmd_assoc = random.choice(ALL_CMDS)
    extra_bcs.append({
        "num_bon_commande": bc_id,
        "date_commande": fmt(d_cmd),
        "id_fournisseur": f_id,
        "statut": statut,
        "details_articles": [{"ref_matiere": mat, "quantite_commandee": qte, "prix_unitaire_achat": prix}],
        "montant_total_ht": ht,
        "montant_total_ttc": ttc,
        "date_livraison_prevue": fmt(d_prev),
        "date_livraison_effective": d_eff,
        "num_bon_livraison": bon_liv,
        "commande_client_associee": cmd_assoc,
    })

all_bcs = BCS_FIXES + extra_bcs
with open(BASE + r"\M2_Achat\achat_bons_commande.json", "w", encoding="utf-8") as f:
    json.dump(all_bcs, f, ensure_ascii=False, indent=2)
print(f"  → achat_bons_commande.json ({len(all_bcs)} bons de commande)")

# ════════════════════════════════════════════════════════════════════
# 5 — STOCK SQL (30 000 mouvements)
# ════════════════════════════════════════════════════════════════════
# On réécrit stock_database.sql intégralement (DDL + données statiques + 30k mouvements)
SQL_DDL = """\
-- ============================================================
-- M1 - Base de données SQL : Service Stock
-- Projet NavalCraft - Équipe SMS INFORMATIQUE
-- ============================================================

DROP TABLE IF EXISTS Alerte_Stock;
DROP TABLE IF EXISTS Mouvement_Stock;
DROP TABLE IF EXISTS Matiere_premiere;
DROP TABLE IF EXISTS Emplacement_Stock;
DROP TABLE IF EXISTS Categorie_Matiere;

CREATE TABLE Categorie_Matiere (
    id_categorie  INT          PRIMARY KEY AUTO_INCREMENT,
    nom_categorie VARCHAR(100) NOT NULL,
    description   TEXT,
    seuil_alerte  DECIMAL(5,2) DEFAULT 20.00,
    unite_mesure  VARCHAR(20)  NOT NULL,
    date_creation TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Emplacement_Stock (
    id_emplacement INT          PRIMARY KEY AUTO_INCREMENT,
    zone           VARCHAR(50)  NOT NULL,
    allee          VARCHAR(10)  NOT NULL,
    rayon          VARCHAR(10)  NOT NULL,
    niveau         VARCHAR(10)  NOT NULL,
    capacite_max   DECIMAL(10,2),
    UNIQUE KEY unique_emplacement (zone, allee, rayon, niveau)
);

CREATE TABLE Matiere_premiere (
    id_matiere               INT           PRIMARY KEY AUTO_INCREMENT,
    code_matiere             VARCHAR(50)   NOT NULL UNIQUE,
    nom_matiere              VARCHAR(150)  NOT NULL,
    description              TEXT,
    id_categorie             INT           NOT NULL,
    id_emplacement           INT,
    quantite_actuelle        DECIMAL(10,2) NOT NULL DEFAULT 0,
    quantite_min             DECIMAL(10,2) NOT NULL,
    quantite_max             DECIMAL(10,2),
    prix_unitaire            DECIMAL(10,2),
    delai_approvisionnement  INT           DEFAULT 7,
    CONSTRAINT fk_matiere_categorie   FOREIGN KEY (id_categorie)  REFERENCES Categorie_Matiere(id_categorie)  ON DELETE RESTRICT,
    CONSTRAINT fk_matiere_emplacement FOREIGN KEY (id_emplacement) REFERENCES Emplacement_Stock(id_emplacement) ON DELETE SET NULL,
    CONSTRAINT chk_quantite_positive  CHECK (quantite_actuelle >= 0)
);

CREATE TABLE Mouvement_Stock (
    id_mouvement        INT           PRIMARY KEY AUTO_INCREMENT,
    id_matiere          INT           NOT NULL,
    type_mouvement      VARCHAR(20)   NOT NULL,
    quantite            DECIMAL(10,2) NOT NULL,
    date_mouvement      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_fournisseur      INT,
    num_bon_livraison   VARCHAR(50),
    num_ordre_livraison VARCHAR(50),
    responsable         VARCHAR(100),
    CONSTRAINT fk_mouvement_matiere  FOREIGN KEY (id_matiere) REFERENCES Matiere_premiere(id_matiere) ON DELETE RESTRICT,
    CONSTRAINT chk_quantite_mouvement CHECK (quantite > 0)
);

CREATE TABLE Alerte_Stock (
    id_alerte         INT           PRIMARY KEY AUTO_INCREMENT,
    id_matiere        INT           NOT NULL,
    type_alerte       VARCHAR(20)   NOT NULL,
    date_alerte       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quantite_actuelle DECIMAL(10,2) NOT NULL,
    quantite_seuil    DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_alerte_matiere FOREIGN KEY (id_matiere) REFERENCES Matiere_premiere(id_matiere) ON DELETE CASCADE
);

-- ============================================================
-- Données statiques
-- ============================================================

INSERT INTO Categorie_Matiere (nom_categorie, description, seuil_alerte, unite_mesure) VALUES
    ('Bois',          'Bois pour construction navale',           18.00, 'm3'),
    ('Resine',        'Resines et colles pour stratification',   21.00, 'L'),
    ('Metal',         'Metaux et alliages marins',               50.00, 'm2'),
    ('Peinture',      'Peintures et traitements de surface',     30.00, 'L'),
    ('Quincaillerie', 'Visserie et accessoires marins inox',    100.00, 'piece'),
    ('Composite',     'Tissus et materiaux composites',           8.00, 'm2');

INSERT INTO Emplacement_Stock (zone, allee, rayon, niveau, capacite_max) VALUES
    ('2B', '01', 'R01', 'N3', 500.00),
    ('9S', '02', 'R01', 'N0',  50.00),
    ('3A', '01', 'R02', 'N1', 200.00),
    ('1C', '03', 'R01', 'N2', 300.00),
    ('4D', '01', 'R01', 'N0', 400.00),
    ('5E', '02', 'R03', 'N1', 150.00),
    ('6F', '01', 'R04', 'N2', 350.00),
    ('7G', '03', 'R02', 'N0', 250.00);

INSERT INTO Matiere_premiere (code_matiere, nom_matiere, description, id_categorie, id_emplacement, quantite_actuelle, quantite_min, quantite_max, prix_unitaire, delai_approvisionnement) VALUES
    ('BOIS-001', 'Chene',               'Planches de chene 50mm',           1, 1,  12.50,   5.00,  40.00, 450.00, 14),
    ('BOIS-002', 'Contreplaque marine', 'Contreplaque marine certifiee',    1, 5, 180.00,  50.00, 500.00, 180.00, 10),
    ('RES-001',  'Resine epoxy',        'Resine epoxy bicomposant',         2, 2, 450.00, 200.00,1000.00,  25.00, 10),
    ('ALU-001',  'Aluminium 5083',      'Tole aluminium marine 5mm',        3, 3,  80.00,  30.00, 250.00, 120.00, 21),
    ('PEI-001',  'Antifouling marine',  'Peinture antifouling rouge',       4, 4,  45.00,  20.00, 100.00,  35.00,  7),
    ('VIS-001',  'Visserie inox marine','Vis et boulons inox A4 assortis',  5, 6,2000.00, 500.00,5000.00,   0.80,  5),
    ('ETR-001',  'Profile acier',       'Profile acier galvanise 40x40mm', 3, 7, 120.00,  30.00, 400.00,  95.00, 14),
    ('FIL-001',  'Filin acier',         'Filin acier galvanise 6mm',        5, 8,  35.00,  10.00, 100.00,  42.00,  7),
    ('CAR-001',  'Tissu fibre carbone', 'Tissu carbone 200g/m2 sergé 2x2', 6, 5,  60.00,  15.00, 200.00, 320.00, 21),
    ('MAS-001',  'Mastic etancheite',   'Mastic polysulfure bicomposant',   2, 6,  80.00,  20.00, 300.00,  15.00,  7);

INSERT INTO Alerte_Stock (id_matiere, type_alerte, date_alerte, quantite_actuelle, quantite_seuil) VALUES
    (1, 'SEUIL_MIN', '2024-03-15 08:00:00',  8.50,   5.00),
    (2, 'SEUIL_MIN', '2024-07-22 08:00:00', 180.00, 200.00),
    (3, 'SEUIL_MIN', '2025-01-10 08:00:00',  25.00,  30.00),
    (4, 'SEUIL_MIN', '2025-05-18 08:00:00',  18.00,  20.00),
    (9, 'SEUIL_MIN', '2025-11-03 08:00:00',  12.00,  15.00);

-- ============================================================
-- 30 000 mouvements de stock générés
-- ============================================================
"""

print("Génération des 30 000 mouvements SQL...")
responsables = ["Alice", "Bruno", "Camille", "David", "Emma", "Felix", "Grace", "Hugo"]
mv_lines = []
bon_num  = 1000

# Distribution : 3000 mouvements par matière (75% ENTREE, 25% SORTIE)
# Étalés sur 2021-2026 pour montrer une vraie historique
for mat_code, _, _, _, prix, f_id in MATIERES:
    mat_id = MAT_IDS[mat_code]
    for j in range(3000):
        bon_num += 1
        type_mv = "ENTREE" if random.random() < 0.75 else "SORTIE"
        qte  = round(random.uniform(5, 250), 2)
        d    = rand_date("2021-01-01", "2026-02-28")
        resp = random.choice(responsables)
        if type_mv == "ENTREE":
            mv_lines.append(
                f"INSERT INTO Mouvement_Stock (id_matiere,type_mouvement,quantite,date_mouvement,id_fournisseur,num_bon_livraison,responsable) "
                f"VALUES ({mat_id},'ENTREE',{qte},'{d} 09:00:00',{f_id},'ENT-{bon_num:07d}','{resp}');"
            )
        else:
            mv_lines.append(
                f"INSERT INTO Mouvement_Stock (id_matiere,type_mouvement,quantite,date_mouvement,num_ordre_livraison,responsable) "
                f"VALUES ({mat_id},'SORTIE',{qte},'{d} 10:00:00','SOR-{bon_num:07d}','{resp}');"
            )

sql_final = SQL_DDL + "\n".join(mv_lines) + "\n"
with open(BASE + r"\M1_Stock\stock_database.sql", "w", encoding="utf-8") as f:
    f.write(sql_final)
print(f"  → stock_database.sql ({len(mv_lines)} mouvements)")

# ════════════════════════════════════════════════════════════════════
# BILAN
# ════════════════════════════════════════════════════════════════════
n_ordres = len(ORDRES_FIXES) + len(extra_ordres)
n_nomen  = len(NOMEN_FIXES)  + len(extra_nomen)
print()
print("=" * 55)
print("  BILAN — nœuds attendus dans Neo4j")
print("=" * 55)
print(f"  Clients                : {len(all_clients):>6}")
print(f"  Commandes              : {total_cmds:>6}")
print(f"  Ordres de fabrication  : {n_ordres:>6}")
print(f"  Bateaux                : {n_ordres:>6}")
print(f"  Bons de commande       : {len(all_bcs):>6}")
print(f"  Fournisseurs           : {5:>6}")
print(f"  Matières premières     : {10:>6}")
print(f"  Mouvements de stock    : {len(mv_lines):>6}")
print(f"  Alertes stock          : {5:>6}")
print(f"  ─────────────────────────────")
total_nodes = len(all_clients) + total_cmds + n_ordres * 2 + len(all_bcs) + 5 + 10 + len(mv_lines) + 5
print(f"  TOTAL nœuds estimé     : {total_nodes:>6}")
print(f"  Relations NECESSITE    : {n_nomen:>6}")
print("=" * 55)
