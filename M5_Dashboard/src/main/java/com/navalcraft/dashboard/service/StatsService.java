package com.navalcraft.dashboard.service;

import com.navalcraft.dashboard.dto.KpiDto;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class StatsService {

    private static final String DB = "navalcraft";
    private final Neo4jClient neo4jClient;

    public StatsService(Neo4jClient neo4jClient) {
        this.neo4jClient = neo4jClient;
    }

    public KpiDto getKpis() {
        long nbMatieres    = count("MatierePremiere");
        long nbFournisseurs = count("Fournisseur");
        long nbClients     = count("Client");
        long nbCommandes   = count("Commande");
        long nbOrdres      = count("OrdreFabrication");
        long nbAlertes     = count("AlerteStock");

        Map<String, Long> commandesParStatut = fetchStatutMap("Commande");
        Map<String, Long> ordresParStatut    = fetchStatutMap("OrdreFabrication");

        return new KpiDto(
                nbMatieres, nbFournisseurs, nbClients,
                nbCommandes, nbOrdres, nbAlertes,
                new ArrayList<>(commandesParStatut.keySet()),
                new ArrayList<>(commandesParStatut.values()),
                new ArrayList<>(ordresParStatut.keySet()),
                new ArrayList<>(ordresParStatut.values())
        );
    }

    private long count(String label) {
        return neo4jClient
                .query("MATCH (n:" + label + ") RETURN count(n) AS cnt")
                .in(DB)
                .fetchAs(Long.class)
                .mappedBy((ts, r) -> r.get("cnt").asLong())
                .one()
                .orElse(0L);
    }

    private Map<String, Long> fetchStatutMap(String label) {
        Collection<Map<String, Object>> rows = neo4jClient
                .query("MATCH (n:" + label + ") WHERE n.statut IS NOT NULL " +
                       "RETURN n.statut AS statut, count(n) AS cnt " +
                       "ORDER BY statut")
                .in(DB)
                .fetch()
                .all();

        Map<String, Long> result = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            result.put((String) row.get("statut"), (Long) row.get("cnt"));
        }
        return result;
    }
}
