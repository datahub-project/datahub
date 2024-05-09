package com.linkedin.metadata.test.hooks;

import static com.linkedin.metadata.Constants.TEST_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.test.util.TestMd5;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestInfo;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/** Hook to populate the md5 hash on metadata tests */
@Setter
@Getter
@Accessors(chain = true)
public class MetadataTestHash extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {

    Set<Urn> mutated =
        items.stream()
            .filter(MetadataTestHash::mutationFilter)
            .map(
                item -> {
                  TestDefinition testDefinition = item.getAspect(TestInfo.class).getDefinition();
                  testDefinition.setMd5(
                      TestMd5.getMd5(testDefinition.getJson()), SetMode.IGNORE_NULL);
                  return item.getUrn();
                })
            .collect(Collectors.toSet());

    return items.stream().map(i -> Pair.of(i, mutated.contains(i.getUrn())));
  }

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (mutationFilter(item)) {
        TestInfo testInfo = item.getAspect(TestInfo.class);
        TestDefinition testDefinition = testInfo.getDefinition();
        testDefinition.setMd5(TestMd5.getMd5(testDefinition.getJson()));
        results.add(Pair.of(item, true));
        continue;
      }

      // no op
      results.add(Pair.of(item, false));
    }

    return results.stream();
  }

  private static boolean mutationFilter(ReadItem item) {
    if (TEST_INFO_ASPECT_NAME.equals(item.getAspectName()) && item.getRecordTemplate() != null) {
      TestInfo testInfo = item.getAspect(TestInfo.class);
      if (testInfo.getDefinition() != null) {
        TestDefinition testDefinition = testInfo.getDefinition();
        return testDefinition.getMd5() == null && testDefinition.getJson() != null;
      }
    }
    return false;
  }
}
