package com.navalcraft.dashboard.controller;

import com.navalcraft.dashboard.service.StatsService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DashboardController {

    private final StatsService statsService;

    public DashboardController(StatsService statsService) {
        this.statsService = statsService;
    }

    @GetMapping("/")
    public String dashboard(Model model) {
        model.addAttribute("kpis", statsService.getKpis());
        model.addAttribute("currentPage", "dashboard");
        return "dashboard";
    }
}
