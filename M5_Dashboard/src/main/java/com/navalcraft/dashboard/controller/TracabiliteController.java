package com.navalcraft.dashboard.controller;

import com.navalcraft.dashboard.service.ServiceDataService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class TracabiliteController {

    private final ServiceDataService dataService;

    public TracabiliteController(ServiceDataService dataService) {
        this.dataService = dataService;
    }

    @GetMapping("/tracabilite")
    public String tracabilite(
            @RequestParam(required = false) String cmd,
            Model model) {

        model.addAttribute("commandes", dataService.getCommandesAvecClients());
        model.addAttribute("selectedCmd", cmd != null ? cmd : "");

        if (cmd != null && !cmd.isBlank()) {
            model.addAttribute("chaine", dataService.getTracabilite(cmd));
        }

        model.addAttribute("currentPage", "tracabilite");
        return "tracabilite";
    }
}
