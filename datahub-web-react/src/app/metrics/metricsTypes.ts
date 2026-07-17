import { GetSemanticModelMetricsQuery, GetSemanticModelsBrowseQuery } from '@graphql/metricsBrowse.generated';

export type SemanticModel = NonNullable<
    NonNullable<GetSemanticModelsBrowseQuery['getSemanticModels']>['semanticModels'][number]
>;

type SearchResultEntity = NonNullable<
    NonNullable<
        NonNullable<GetSemanticModelMetricsQuery['semanticModel']>['metrics']
    >['searchResults'][number]['entity']
>;
export type MetricEntity = Extract<SearchResultEntity, { __typename?: 'Metric' }>;
