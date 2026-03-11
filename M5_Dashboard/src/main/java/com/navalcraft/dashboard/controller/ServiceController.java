package com.navalcraft.dashboard.controller;

import com.navalcraft.dashboard.service.ServiceDataService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ServiceController {

    private final ServiceDataService dataService;

    public ServiceController(ServiceDataService dataService) {
        this.dataService = dataService;
    }

    @GetMapping("/stock")
    public String stock(Model model) {
        model.addAttribute("matieres",   dataService.getMatieres());
        model.addAttribute("mouvements", dataService.getMouvements());
        model.addAttribute("currentPage", "stock");
        return "stock";
    }

    @GetMapping("/achat")
    public String achat(Model model) {
        model.addAttribute("fournisseurs",  dataService.getFournisseurs());
        model.addAttribute("bonsCommande",  dataService.getBonsCommande());
        model.addAttribute("currentPage", "achat");
        return "achat";
    }

    @GetMapping("/vente")
    public String vente(Model model) {
        model.addAttribute("clients",   dataService.getClients());
        model.addAttribute("commandes", dataService.getCommandes());
        model.addAttribute("currentPage", "vente");
        return "vente";
    }

    @GetMapping("/production")
    public String production(Model model) {
        model.addAttribute("ordres",   dataService.getOrdres());
        model.addAttribute("bateaux",  dataService.getBateaux());
        model.addAttribute("currentPage", "production");
        return "production";
    }
}
