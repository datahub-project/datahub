package com.linkedin.gms.factory.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.datahub.telemetry.TrackingDestination;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.ClientDelivery;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.EnumSet;
import java.util.Optional;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link MixpanelApiFactory}, {@link MixpanelMessageBuilderFactory} and {@link
 * TrackingServiceFactory} with the production {@code application.yaml} and checks where {@code
 * trackingService} sends events under {@code telemetry.enabledServer}. The Mixpanel client built by
 * the factory is spied so that no event leaves the test.
 */
public class TrackingServiceFactoryContextTest {

  private static final String EVENT = "TestEvent";
  private static final long DELIVERY_WAIT_MS = 10_000;
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  /**
   * Spies the factory-built Mixpanel client and stops it from sending over the network. Registered
   * through an initializer, not a nested {@code @Configuration}, so the component scan in {@link
   * TrackingServiceFactoryTest} does not pick it up.
   */
  private static final BeanPostProcessor OFFLINE_MIXPANEL =
      new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
          if (!(bean instanceof MixpanelAPI mixpanel)) {
            return bean;
          }
          MixpanelAPI offline = spy(mixpanel);
          try {
            // MixpanelEventSender delivers batches through deliver(); sendMessage is stubbed too
            // so no code path can reach the network.
            doNothing().when(offline).deliver(any(ClientDelivery.class));
            doNothing().when(offline).sendMessage(any());
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
          return offline;
        }
      };

  private static ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            MixpanelApiFactory.class,
            MixpanelMessageBuilderFactory.class,
            TrackingServiceFactory.class)
        .withInitializer(context -> context.getBeanFactory().addBeanPostProcessor(OFFLINE_MIXPANEL))
        .withBean(
            "dataHubSecretService", SecretService.class, () -> new SecretService("secret", false))
        .withBean("entityService", EntityService.class, () -> mock(EntityService.class))
        .withBean(
            "gitVersion",
            GitVersion.class,
            () -> new GitVersion("v1.0.0", "abc", Optional.empty()));
  }

  @Test
  public void testTelemetryIsOnByDefaultAndSendsToMixpanel() throws Exception {
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasBean("mixpanelApi");
              assertThat(trackingService(context).track(EVENT, OP_CONTEXT, null, null, event()))
                  .isEqualTo(1);

              // Events are queued and delivered by MixpanelEventSender's background thread, so wait
              // for the delivery instead of asserting right after track() returns.
              verify(context.getBean("mixpanelApi", MixpanelAPI.class), timeout(DELIVERY_WAIT_MS))
                  .deliver(any(ClientDelivery.class));
            });
  }

  @Test
  public void testTelemetryDisabledCreatesNoMixpanelClient() {
    runner()
        .withPropertyValues("telemetry.enabledServer=false")
        .run(
            context -> {
              assertThat(context)
                  .hasNotFailed()
                  .doesNotHaveBean("mixpanelApi")
                  .doesNotHaveBean("mixpanelMessageBuilder");
              assertThat(
                      trackingService(context)
                          .track(
                              EVENT,
                              OP_CONTEXT,
                              null,
                              null,
                              event(),
                              EnumSet.of(TrackingDestination.MIXPANEL)))
                  .isZero();
            });
  }

  @Test
  public void testUsageEventsGoToTheConfiguredTopicWhenAPublisherExists() {
    UsageEventPublisher publisher = mock(UsageEventPublisher.class);
    runner()
        .withPropertyValues(
            "telemetry.enabledServer=false", "kafka.topics.datahubUsageEvent.name=my-usage-topic")
        .withBean("dataHubUsageEventProducer", UsageEventPublisher.class, () -> publisher)
        .run(
            context -> {
              assertThat(trackingService(context).track(EVENT, OP_CONTEXT, null, null, event()))
                  .isEqualTo(1);
              verify(publisher).publish(any(), eq("my-usage-topic"), anyString(), anyString());
            });
  }

  @Test
  public void testWithoutAPublisherKafkaIsSkipped() {
    runner()
        .withPropertyValues("telemetry.enabledServer=false")
        .run(
            context ->
                assertThat(
                        trackingService(context)
                            .track(
                                EVENT,
                                OP_CONTEXT,
                                null,
                                null,
                                event(),
                                EnumSet.of(TrackingDestination.KAFKA)))
                    .isZero());
  }

  private static TrackingService trackingService(ApplicationContext context) {
    return context.getBean("trackingService", TrackingService.class);
  }

  private static JsonNode event() {
    return JsonNodeFactory.instance.objectNode().put("type", EVENT);
  }
}
