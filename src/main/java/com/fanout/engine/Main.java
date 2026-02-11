package com.fanout.engine;

import com.fanout.engine.config.AppConfig;
import com.fanout.engine.config.ConfigLoader;
import com.fanout.engine.core.EngineRunResult;
import com.fanout.engine.core.FanOutEngine;

import java.nio.file.Path;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config/application.yaml";
        AppConfig config = ConfigLoader.load(Path.of(configPath));

        FanOutEngine engine = new FanOutEngine(config);
        EngineRunResult result = engine.run();

        System.out.println("[done] recordsIngested=" + result.recordsIngested()
                + " expectedDeliveries=" + result.expectedDeliveries()
                + " accountedDeliveries=" + result.accountedDeliveries());
    }
}
