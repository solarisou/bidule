-- ============================================================
-- M1 - Base de données SQL : Service Stock
-- Projet NavalCraft - Équipe SMS INFORMATIQUE
-- GHERRAS Salman • CHERIFI Sarah • ALI ASSOUMANE Mtara
-- Date : 25 Février 2026
-- ============================================================

-- Suppression des tables (ordre inverse des dépendances)
DROP TABLE IF EXISTS Alerte_Stock;
DROP TABLE IF EXISTS Mouvement_Stock;
DROP TABLE IF EXISTS Matiere_premiere;
DROP TABLE IF EXISTS Emplacement_Stock;
DROP TABLE IF EXISTS Categorie_Matiere;

-- ============================================================
-- 2.4.1 Création des tables
-- ============================================================

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
    id_matiere               INT          PRIMARY KEY AUTO_INCREMENT,
    code_matiere             VARCHAR(50)  NOT NULL UNIQUE,
    nom_matiere              VARCHAR(150) NOT NULL,
    description              TEXT,
    id_categorie             INT          NOT NULL,
    id_emplacement           INT,
    quantite_actuelle        DECIMAL(10,2) NOT NULL DEFAULT 0,
    quantite_min             DECIMAL(10,2) NOT NULL,
    quantite_max             DECIMAL(10,2),
    prix_unitaire            DECIMAL(10,2),
    delai_approvisionnement  INT          DEFAULT 7,
    CONSTRAINT fk_matiere_categorie   FOREIGN KEY (id_categorie)
        REFERENCES Categorie_Matiere(id_categorie) ON DELETE RESTRICT,
    CONSTRAINT fk_matiere_emplacement FOREIGN KEY (id_emplacement)
        REFERENCES Emplacement_Stock(id_emplacement) ON DELETE SET NULL,
    CONSTRAINT chk_quantite_positive  CHECK (quantite_actuelle >= 0)
);

CREATE TABLE Mouvement_Stock (
    id_mouvement       INT          PRIMARY KEY AUTO_INCREMENT,
    id_matiere         INT          NOT NULL,
    type_mouvement     VARCHAR(20)  NOT NULL,
    quantite           DECIMAL(10,2) NOT NULL,
    date_mouvement     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_fournisseur     INT,
    num_bon_livraison  VARCHAR(50),
    num_ordre_livraison VARCHAR(50),
    responsable        VARCHAR(100),
    CONSTRAINT fk_mouvement_matiere  FOREIGN KEY (id_matiere)
        REFERENCES Matiere_premiere(id_matiere) ON DELETE RESTRICT,
    CONSTRAINT chk_quantite_mouvement CHECK (quantite > 0)
);

CREATE TABLE Alerte_Stock (
    id_alerte         INT          PRIMARY KEY AUTO_INCREMENT,
    id_matiere        INT          NOT NULL,
    type_alerte       VARCHAR(20)  NOT NULL,
    date_alerte       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quantite_actuelle DECIMAL(10,2) NOT NULL,
    quantite_seuil    DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_alerte_matiere FOREIGN KEY (id_matiere)
        REFERENCES Matiere_premiere(id_matiere) ON DELETE CASCADE
);

-- ============================================================
-- 2.4.2 Insertion des données fictives
-- ============================================================

-- Catégories de matières premières
INSERT INTO Categorie_Matiere
    (nom_categorie, description, seuil_alerte, unite_mesure) VALUES
    ('Bois',     'Bois pour construction navale',         18.00, 'm3'),
    ('Resine',   'Resines et colles pour stratification', 21.00, 'L'),
    ('Metal',    'Metaux et alliages marins',             50.00, 'm2'),
    ('Peinture', 'Peintures et traitements de surface',   30.00, 'L');

-- Emplacements physiques dans l'entrepôt
INSERT INTO Emplacement_Stock
    (zone, allee, rayon, niveau, capacite_max) VALUES
    ('2B', '01', 'R01', 'N3', 500.00),
    ('9S', '02', 'R01', 'N0',  50.00),
    ('3A', '01', 'R02', 'N1', 200.00),
    ('1C', '03', 'R01', 'N2', 300.00);

-- Matières premières
INSERT INTO Matiere_premiere (
    code_matiere, nom_matiere, description,
    id_categorie, id_emplacement,
    quantite_actuelle, quantite_min, quantite_max,
    prix_unitaire, delai_approvisionnement) VALUES
    ('BOIS-001', 'Chene',             'Planches de chene 50mm',          1, 1,  12.50,   5.00,   40.00, 450.00, 14),
    ('RES-001',  'Resine',            'Resine pour stratification',       2, 2, 450.00, 200.00, 1000.00,  25.00, 10),
    ('ALU-001',  'Aluminium 5083',    'Tole aluminium marine 5mm',        3, 3,  80.00,  30.00,  250.00, 120.00, 21),
    ('PEI-001',  'Antifouling marine','Peinture antifouling rouge',       4, 4,  45.00,  20.00,  100.00,  35.00,  7);

-- Mouvements de stock - Entrée BOIS-001 (livraison fournisseur F001)
INSERT INTO Mouvement_Stock (
    id_matiere, type_mouvement, quantite,
    date_mouvement, id_fournisseur, num_bon_livraison, responsable)
VALUES
    (1, 'ENTREE', 15.00, '2026-01-20 10:30:00', 1, 'ENT-2026-001', 'Soso');

-- Mouvements de stock - Sortie RES-001 (consommation production)
INSERT INTO Mouvement_Stock (
    id_matiere, type_mouvement, quantite,
    date_mouvement, num_ordre_livraison, responsable)
VALUES
    (2, 'SORTIE', 50.00, '2026-02-01 11:00:00', 'SOR-2026-001', 'Lala');

-- Entrée RES-001 (livraison fournisseur F002)
INSERT INTO Mouvement_Stock (
    id_matiere, type_mouvement, quantite,
    date_mouvement, id_fournisseur, num_bon_livraison, responsable)
VALUES
    (2, 'ENTREE', 300.00, '2026-01-25 09:00:00', 2, 'ENT-2026-002', 'Soso');

-- Sortie ALU-001 (consommation ordre OF-2026-001)
INSERT INTO Mouvement_Stock (
    id_matiere, type_mouvement, quantite,
    date_mouvement, num_ordre_livraison, responsable)
VALUES
    (3, 'SORTIE', 25.00, '2026-02-10 14:00:00', 'SOR-2026-002', 'Lala');

-- Alertes de stock
INSERT INTO Alerte_Stock (
    id_matiere, type_alerte, date_alerte,
    quantite_actuelle, quantite_seuil) VALUES
    (1, 'SEUIL_MIN', '2026-02-08 08:00:00', 12.50,   5.00),
    (2, 'SEUIL_MIN', '2026-02-09 08:00:00', 450.00, 200.00);
