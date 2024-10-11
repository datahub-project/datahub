package com.datahub.plugins.metadata.aspect.test;

import com.datahub.plugins.metadata.aspect.SpringPluginFactoryTest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** This is the Spring class used for the test */
@Configuration
public class TestSpringPluginConfiguration {
  @Bean
  public SpringPluginFactoryTest.TestValidator springValidator1() {
    SpringPluginFactoryTest.TestValidator testValidator =
        new SpringPluginFactoryTest.TestValidator();
    testValidator.setConfig(
        AspectPluginConfig.builder()
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("dataset")
                        .aspectName("status")
                        .build()))
            .className(SpringPluginFactoryTest.TestValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of(ChangeType.UPSERT.toString()))
            .build());
    return testValidator;
  }

  @Bean
  public SpringPluginFactoryTest.TestValidator springValidator2() {
    SpringPluginFactoryTest.TestValidator testValidator =
        new SpringPluginFactoryTest.TestValidator();
    testValidator.setConfig(
        AspectPluginConfig.builder()
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName("status")
                        .build()))
            .className(SpringPluginFactoryTest.TestValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of(ChangeType.DELETE.toString()))
            .build());
    return testValidator;
  }

  @Bean
  public SpringPluginFactoryTest.TestMutator springMutator() {
    SpringPluginFactoryTest.TestMutator testMutator = new SpringPluginFactoryTest.TestMutator();
    testMutator.setConfig(
        AspectPluginConfig.builder()
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName("schemaMetadata")
                        .build()))
            .className(SpringPluginFactoryTest.TestMutator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of(ChangeType.UPSERT.toString(), ChangeType.DELETE.toString()))
            .build());
    return testMutator;
  }

  @Bean
  public SpringPluginFactoryTest.TestMCPSideEffect springMCPSideEffect() {
    SpringPluginFactoryTest.TestMCPSideEffect testMCPSideEffect =
        new SpringPluginFactoryTest.TestMCPSideEffect();
    testMCPSideEffect.setConfig(
        AspectPluginConfig.builder()
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("dataset")
                        .aspectName("datasetKey")
                        .build()))
            .className(SpringPluginFactoryTest.TestMCPSideEffect.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of(ChangeType.UPSERT.toString(), ChangeType.DELETE.toString()))
            .build());
    return testMCPSideEffect;
  }

  @Bean
  public SpringPluginFactoryTest.TestMCLSideEffect springMCLSideEffect() {
    SpringPluginFactoryTest.TestMCLSideEffect testMCLSideEffect =
        new SpringPluginFactoryTest.TestMCLSideEffect();
    testMCLSideEffect.setConfig(
        AspectPluginConfig.builder()
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("chart")
                        .aspectName("ChartInfo")
                        .build()))
            .className(SpringPluginFactoryTest.TestMCLSideEffect.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of(ChangeType.UPSERT.toString(), ChangeType.DELETE.toString()))
            .build());
    return testMCLSideEffect;
  }
}
