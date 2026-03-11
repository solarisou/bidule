package com.navalcraft.dashboard.service;

import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Service
public class ServiceDataService {

    private static final String DB = "navalcraft";
    private final Neo4jClient neo4jClient;

    public ServiceDataService(Neo4jClient neo4jClient) {
        this.neo4jClient = neo4jClient;
    }

    // ── M1 STOCK ──────────────────────────────────────────────────────────────

    public List<Map<String, Object>> getMatieres() {
        return query("""
            MATCH (m:MatierePremiere)
            OPTIONAL MATCH (m)-[:APPARTIENT_A]->(c:Categorie)
            OPTIONAL MATCH (m)-[:STOCKEE_EN]->(e:Emplacement)
            RETURN m.code AS code, m.nom AS nom,
                   m.quantite_stock AS quantite, m.unite AS unite,
                   m.seuil_alerte AS seuil,
                   coalesce(c.nom, '-') AS categorie,
                   coalesce(e.allee, '-') AS emplacement
            ORDER BY m.code
            """);
    }

    public List<Map<String, Object>> getMouvements() {
        return query("""
            MATCH (mv:MouvementStock)-[:CONCERNE]->(m:MatierePremiere)
            RETURN mv.type_mouvement AS type, mv.quantite AS quantite,
                   mv.date_mouvement AS date, m.code AS matiere
            ORDER BY mv.date_mouvement DESC
            LIMIT 1000
            """);
    }

    // ── M2 ACHAT ──────────────────────────────────────────────────────────────

    public List<Map<String, Object>> getFournisseurs() {
        return query("""
            MATCH (f:Fournisseur)
            RETURN f.id AS id, f.nom AS nom,
                   coalesce(f.contact, '-') AS contact,
                   coalesce(f.email, '-') AS email,
                   coalesce(f.delai_livraison_j, '-') AS delai
            ORDER BY f.id
            """);
    }

    public List<Map<String, Object>> getBonsCommande() {
        return query("""
            MATCH (bc:BonCommande)
            OPTIONAL MATCH (bc)-[:ADRESSE_A]->(f:Fournisseur)
            RETURN bc.id AS id, bc.statut AS statut,
                   bc.date_commande AS date,
                   coalesce(f.nom, '-') AS fournisseur
            ORDER BY bc.date_commande DESC
            LIMIT 1000
            """);
    }

    // ── M3 VENTE ──────────────────────────────────────────────────────────────

    public List<Map<String, Object>> getClients() {
        return query("""
            MATCH (c:Client)
            RETURN c.id AS id, c.nom AS nom, c.prenom AS prenom,
                   coalesce(c.email, '-') AS email,
                   coalesce(c.telephone, '-') AS telephone
            ORDER BY c.id
            """);
    }

    public List<Map<String, Object>> getCommandes() {
        return query("""
            MATCH (cmd:Commande)
            OPTIONAL MATCH (c:Client)-[:A_PASSE]->(cmd)
            RETURN cmd.id AS id, cmd.statut AS statut,
                   cmd.date_commande AS date_commande,
                   cmd.date_livraison_prevue AS date_livraison,
                   coalesce(toString(cmd.montant_total_paye), '-') AS montant,
                   coalesce(c.nom + ' ' + c.prenom, '-') AS client
            ORDER BY cmd.date_commande DESC
            LIMIT 1000
            """);
    }

    // ── M4 PRODUCTION ─────────────────────────────────────────────────────────

    public List<Map<String, Object>> getOrdres() {
        return query("""
            MATCH (of:OrdreFabrication)
            OPTIONAL MATCH (of)-[:PRODUIT]->(b:Bateau)
            RETURN of.id AS id, of.statut AS statut,
                   of.date_creation AS date_creation,
                   of.date_debut_prevue AS date_debut,
                   of.date_fin_prevue AS date_fin,
                   coalesce(b.ref, '-') AS bateau
            ORDER BY of.date_creation DESC
            LIMIT 1000
            """);
    }

    public List<Map<String, Object>> getBateaux() {
        return query("""
            MATCH (b:Bateau)
            OPTIONAL MATCH (of:OrdreFabrication)-[:PRODUIT]->(b)
            RETURN b.ref AS ref, coalesce(of.statut, '-') AS statut_of
            ORDER BY b.ref
            LIMIT 1000
            """);
    }

    // ── TRACABILITE (inter-services) ──────────────────────────────────────────

    public List<Map<String, Object>> getCommandesAvecClients() {
        return query("""
            MATCH (cl:Client)-[:A_PASSE]->(cmd:Commande)
            WHERE cmd.statut IS NOT NULL
            RETURN cmd.id AS id, cl.nom + ' ' + cl.prenom AS client
            ORDER BY cmd.id
            """);
    }

    public List<Map<String, Object>> getTracabilite(String cmdId) {
        Collection<Map<String, Object>> result = neo4jClient
                .query("""
                    MATCH (cl:Client)-[:A_PASSE]->(cmd:Commande {id: $cmdId})
                    OPTIONAL MATCH (cmd)-[:DECLENCHE]->(of:OrdreFabrication)
                    OPTIONAL MATCH (of)-[:NECESSITE]->(m:MatierePremiere)
                    OPTIONAL MATCH (m)<-[:FOURNIT]-(f:Fournisseur)
                    RETURN cl.nom + ' ' + cl.prenom AS client,
                           cmd.id AS commande_id,
                           cmd.statut AS cmd_statut,
                           coalesce(toString(cmd.montant_total_paye), '-') AS montant,
                           coalesce(of.id, '-') AS ordre_id,
                           coalesce(of.statut, '-') AS ordre_statut,
                           coalesce(m.code, '-') AS matiere_code,
                           coalesce(m.nom, '-') AS matiere_nom,
                           coalesce(f.nom, '-') AS fournisseur
                    ORDER BY of.id, m.code
                    """)
                .in(DB)
                .bindAll(Map.of("cmdId", cmdId))
                .fetch()
                .all();
        return List.copyOf(result);
    }

    // ── Utilitaire ────────────────────────────────────────────────────────────

    private List<Map<String, Object>> query(String cypher) {
        Collection<Map<String, Object>> result = neo4jClient
                .query(cypher)
                .in(DB)
                .fetch()
                .all();
        return List.copyOf(result);
    }
}
