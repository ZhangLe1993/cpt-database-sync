package com.aihuishou.bi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Boot implements CommandLineRunner {

    @Value("${database}")
    private String database;

    @Autowired
    private com.aihuishou.bi.handler.Handler handler;

    @Override
    public void run(String... args) throws Exception {
        handler.fetch(database);
    }
}
