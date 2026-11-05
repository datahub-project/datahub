package com.linkedin.gms.factory.search.ranker;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.ranker.SearchRanker;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link SearchRankerFactory} through Spring and checks that its {@code searchRanker} ranks
 * by backend score and wins by-type injection (as {@code SearchServiceFactory} uses) over any other
 * {@link SearchRanker} on the context.
 */
public class SearchRankerFactoryTest {

  @Test
  public void testSearchRankerOrdersByBackendScore() {
    new ApplicationContextRunner()
        .withUserConfiguration(SearchRankerFactory.class)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              SearchRanker<?> ranker = context.getBean("searchRanker", SearchRanker.class);
              assertThat(ranker.rank(List.of(entity("low", 1.0), entity("high", 5.0))))
                  .extracting(entity -> entity.getEntity().getId())
                  .containsExactly("high", "low");
            });
  }

  @Test
  public void testSearchRankerIsPrimaryOverOtherRankers() {
    new ApplicationContextRunner()
        .withUserConfiguration(SearchRankerFactory.class, RankerConsumer.class)
        .withBean("customRanker", SearchRanker.class, ReverseRanker::new)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(RankerConsumer.class).ranker)
                  .isSameAs(context.getBean("searchRanker"));
            });
  }

  static class RankerConsumer {
    @Autowired SearchRanker<?> ranker;
  }

  static class ReverseRanker extends SearchRanker<Double> {
    @Override
    public List<FeatureExtractor> getFeatureExtractors() {
      return List.of();
    }

    @Override
    public Double score(SearchEntity searchEntity) {
      return -searchEntity.getScore();
    }
  }

  private static SearchEntity entity(String id, double score) {
    return new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:tag:" + id)).setScore(score);
  }
}
