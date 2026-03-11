package com.navalcraft.dashboard.dto;

import java.util.List;

public record KpiDto(
        long nbMatieres,
        long nbFournisseurs,
        long nbClients,
        long nbCommandes,
        long nbOrdres,
        long nbAlertes,
        List<String> statutsCommandes,
        List<Long>   countCommandes,
        List<String> statutsOrdres,
        List<Long>   countOrdres
) {}
