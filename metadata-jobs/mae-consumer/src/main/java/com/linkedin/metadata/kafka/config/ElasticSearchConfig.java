package com.linkedin.metadata.kafka.config;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.metadata.builders.search.BaseIndexBuilder;
import com.linkedin.metadata.builders.search.SnapshotProcessor;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Slf4j
@Configuration
@Import({RestHighLevelClientFactory.class, IndexBuildersConfig.class})
public class ElasticSearchConfig {

  @Bean
  public SnapshotProcessor snapshotProcessor(@Nonnull Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders) {
    return new SnapshotProcessor(indexBuilders);
  }
}
