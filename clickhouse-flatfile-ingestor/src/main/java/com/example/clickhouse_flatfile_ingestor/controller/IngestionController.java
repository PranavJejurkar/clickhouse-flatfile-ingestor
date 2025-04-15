package com.example.clickhouse_flatfile_ingestor.controller;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import com.example.clickhouse_flatfile_ingestor.service.ClickHouseService;
import com.example.clickhouse_flatfile_ingestor.service.FileService;

@Controller
public class IngestionController {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FileService fileService;

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/ingest")
    public String ingest(@RequestParam String source,
                         @RequestParam String host,
                         @RequestParam String port,
                         @RequestParam String database,
                         @RequestParam String user,
                         @RequestParam String token,
                         @RequestParam String filepath,
                         @RequestParam String table,
                         Model model) {
        try {
            if (source.equals("clickhouse")) {
                int rows = clickHouseService.exportToCSV(host, port, database, user, token, table, filepath);
                model.addAttribute("message", "Exported " + rows + " rows from ClickHouse to CSV.");
            } else {
                int rows = clickHouseService.importFromCSV(host, port, database, user, token, table, filepath);
                model.addAttribute("message", "Imported " + rows + " rows from CSV to ClickHouse.");
            }
        } catch (Exception e) {
            model.addAttribute("message", "Error: " + e.getMessage());
        }
        return "index";
    }
}
