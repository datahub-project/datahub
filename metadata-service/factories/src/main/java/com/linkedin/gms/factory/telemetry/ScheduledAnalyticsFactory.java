package com.linkedin.gms.factory.telemetry;

import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.mixpanel.mixpanelapi.ClientDelivery;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.json.JSONObject;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import com.linkedin.gms.factory.telemetry.Telemetry;

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {
    private AnalyticsService _analyticsService;



    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void scheduleFixedDelayTask() {

        Telemetry telemetry = new Telemetry();




        log.info(
                "Fixed delay task - " + System.currentTimeMillis() / 1000);
    }
}
