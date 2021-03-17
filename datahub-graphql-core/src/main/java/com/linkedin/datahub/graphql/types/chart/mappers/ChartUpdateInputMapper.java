package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.dashboard.Chart;
import com.linkedin.datahub.graphql.generated.ChartUpdateInput;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class ChartUpdateInputMapper implements ModelMapper<ChartUpdateInput, Chart> {
    public static final ChartUpdateInputMapper INSTANCE = new ChartUpdateInputMapper();

    public static Chart map(@Nonnull final ChartUpdateInput chartUpdateInput) {
        return INSTANCE.apply(chartUpdateInput);
    }

    @Override
    public Chart apply(@Nonnull final ChartUpdateInput chartUpdateInput) {
        final Chart result = new Chart();

        if (chartUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            chartUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }
        return result;
    }

}
