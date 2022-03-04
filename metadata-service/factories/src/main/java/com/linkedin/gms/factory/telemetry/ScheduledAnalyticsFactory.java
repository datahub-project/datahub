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

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {
    private AnalyticsService _analyticsService;

    String PROJECT_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";



//    MessageBuilder messageBuilder =
//            new MessageBuilder(PROJECT_TOKEN);
//    JSONObject sentEvent =
//            messageBuilder.event(distinctId, "weekly-analytics", null);
//
//    // You can send properties along with events
//    JSONObject props = new JSONObject();
//        props.put("Gender","Female");
//        props.put("Plan","Premium");
//    JSONObject planEvent =
//            messageBuilder.event(distinctId, "Plan Selected", props);
//    ClientDelivery delivery = new ClientDelivery();
//    delivery.addMessage(planEvent);
//    MixpanelAPI mixpanel = new MixpanelAPI();
//    mixpanel.deliver(delivery);

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void scheduleFixedDelayTask() {
        log.info(
                "Fixed delay task - " + System.currentTimeMillis() / 1000);
    }
}
