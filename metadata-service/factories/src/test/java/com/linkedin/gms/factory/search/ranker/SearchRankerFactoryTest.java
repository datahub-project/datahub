package com.linkedin.gms.factory.search.ranker;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.ranker.SearchRanker;
import java.util.List;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link SearchRankerFactory} through Spring and checks that the {@code searchRanker} bean,
 * which {@code SearchServiceFactory} injects by that name, ranks results by backend score.
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

  private static SearchEntity entity(String id, double score) {
    return new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:tag:" + id)).setScore(score);
  }
}
