import { ScrollMetricsQuery, ScrollSemanticModelsQuery } from '@graphql/metricsBrowse.generated';

type ScrollMetricSearchResult = NonNullable<
    NonNullable<ScrollMetricsQuery['scrollAcrossEntities']>['searchResults'][number]['entity']
>;
export type MetricEntity = Extract<ScrollMetricSearchResult, { __typename?: 'Metric' }>;

type ScrollSemanticModelSearchResult = NonNullable<
    NonNullable<ScrollSemanticModelsQuery['scrollAcrossEntities']>['searchResults'][number]['entity']
>;
export type SemanticModel = Extract<ScrollSemanticModelSearchResult, { __typename?: 'SemanticModel' }>;
