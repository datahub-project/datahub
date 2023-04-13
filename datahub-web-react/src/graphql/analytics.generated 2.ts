/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type IsAnalyticsEnabledQueryVariables = Types.Exact<{ [key: string]: never }>;

export type IsAnalyticsEnabledQuery = { __typename?: 'Query' } & Pick<Types.Query, 'isAnalyticsEnabled'>;

export type GetAnalyticsChartsQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetAnalyticsChartsQuery = { __typename?: 'Query' } & {
    getAnalyticsCharts: Array<
        { __typename?: 'AnalyticsChartGroup' } & Pick<Types.AnalyticsChartGroup, 'groupId' | 'title'> & {
                charts: Array<
                    | ({ __typename?: 'TimeSeriesChart' } & AnalyticsChart_TimeSeriesChart_Fragment)
                    | ({ __typename?: 'BarChart' } & AnalyticsChart_BarChart_Fragment)
                    | ({ __typename?: 'TableChart' } & AnalyticsChart_TableChart_Fragment)
                >;
            }
    >;
};

export type GetMetadataAnalyticsChartsQueryVariables = Types.Exact<{
    input: Types.MetadataAnalyticsInput;
}>;

export type GetMetadataAnalyticsChartsQuery = { __typename?: 'Query' } & {
    getMetadataAnalyticsCharts: Array<
        { __typename?: 'AnalyticsChartGroup' } & Pick<Types.AnalyticsChartGroup, 'groupId' | 'title'> & {
                charts: Array<
                    | ({ __typename?: 'TimeSeriesChart' } & AnalyticsChart_TimeSeriesChart_Fragment)
                    | ({ __typename?: 'BarChart' } & AnalyticsChart_BarChart_Fragment)
                    | ({ __typename?: 'TableChart' } & AnalyticsChart_TableChart_Fragment)
                >;
            }
    >;
};

export type AnalyticsChart_TimeSeriesChart_Fragment = { __typename?: 'TimeSeriesChart' } & Pick<
    Types.TimeSeriesChart,
    'title' | 'interval'
> & {
        lines: Array<
            { __typename?: 'NamedLine' } & Pick<Types.NamedLine, 'name'> & {
                    data: Array<{ __typename?: 'NumericDataPoint' } & Pick<Types.NumericDataPoint, 'x' | 'y'>>;
                }
        >;
        dateRange: { __typename?: 'DateRange' } & Pick<Types.DateRange, 'start' | 'end'>;
    };

export type AnalyticsChart_BarChart_Fragment = { __typename?: 'BarChart' } & Pick<Types.BarChart, 'title'> & {
        bars: Array<
            { __typename?: 'NamedBar' } & Pick<Types.NamedBar, 'name'> & {
                    segments: Array<{ __typename?: 'BarSegment' } & Pick<Types.BarSegment, 'label' | 'value'>>;
                }
        >;
    };

export type AnalyticsChart_TableChart_Fragment = { __typename?: 'TableChart' } & Pick<
    Types.TableChart,
    'title' | 'columns'
> & {
        rows: Array<
            { __typename?: 'Row' } & Pick<Types.Row, 'values'> & {
                    cells?: Types.Maybe<
                        Array<
                            { __typename?: 'Cell' } & Pick<Types.Cell, 'value'> & {
                                    linkParams?: Types.Maybe<
                                        { __typename?: 'LinkParams' } & {
                                            searchParams?: Types.Maybe<
                                                { __typename?: 'SearchParams' } & Pick<
                                                    Types.SearchParams,
                                                    'types' | 'query'
                                                > & {
                                                        filters?: Types.Maybe<
                                                            Array<
                                                                { __typename?: 'FacetFilter' } & Pick<
                                                                    Types.FacetFilter,
                                                                    'field' | 'values'
                                                                >
                                                            >
                                                        >;
                                                    }
                                            >;
                                            entityProfileParams?: Types.Maybe<
                                                { __typename?: 'EntityProfileParams' } & Pick<
                                                    Types.EntityProfileParams,
                                                    'urn' | 'type'
                                                >
                                            >;
                                        }
                                    >;
                                }
                        >
                    >;
                }
        >;
    };

export type AnalyticsChartFragment =
    | AnalyticsChart_TimeSeriesChart_Fragment
    | AnalyticsChart_BarChart_Fragment
    | AnalyticsChart_TableChart_Fragment;

export const AnalyticsChartFragmentDoc = gql`
    fragment analyticsChart on AnalyticsChart {
        ... on TimeSeriesChart {
            title
            lines {
                name
                data {
                    x
                    y
                }
            }
            dateRange {
                start
                end
            }
            interval
        }
        ... on BarChart {
            title
            bars {
                name
                segments {
                    label
                    value
                }
            }
        }
        ... on TableChart {
            title
            columns
            rows {
                values
                cells {
                    value
                    linkParams {
                        searchParams {
                            types
                            query
                            filters {
                                field
                                values
                            }
                        }
                        entityProfileParams {
                            urn
                            type
                        }
                    }
                }
            }
        }
    }
`;
export const IsAnalyticsEnabledDocument = gql`
    query isAnalyticsEnabled {
        isAnalyticsEnabled
    }
`;

/**
 * __useIsAnalyticsEnabledQuery__
 *
 * To run a query within a React component, call `useIsAnalyticsEnabledQuery` and pass it any options that fit your needs.
 * When your component renders, `useIsAnalyticsEnabledQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useIsAnalyticsEnabledQuery({
 *   variables: {
 *   },
 * });
 */
export function useIsAnalyticsEnabledQuery(
    baseOptions?: Apollo.QueryHookOptions<IsAnalyticsEnabledQuery, IsAnalyticsEnabledQueryVariables>,
) {
    return Apollo.useQuery<IsAnalyticsEnabledQuery, IsAnalyticsEnabledQueryVariables>(
        IsAnalyticsEnabledDocument,
        baseOptions,
    );
}
export function useIsAnalyticsEnabledLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<IsAnalyticsEnabledQuery, IsAnalyticsEnabledQueryVariables>,
) {
    return Apollo.useLazyQuery<IsAnalyticsEnabledQuery, IsAnalyticsEnabledQueryVariables>(
        IsAnalyticsEnabledDocument,
        baseOptions,
    );
}
export type IsAnalyticsEnabledQueryHookResult = ReturnType<typeof useIsAnalyticsEnabledQuery>;
export type IsAnalyticsEnabledLazyQueryHookResult = ReturnType<typeof useIsAnalyticsEnabledLazyQuery>;
export type IsAnalyticsEnabledQueryResult = Apollo.QueryResult<
    IsAnalyticsEnabledQuery,
    IsAnalyticsEnabledQueryVariables
>;
export const GetAnalyticsChartsDocument = gql`
    query getAnalyticsCharts {
        getAnalyticsCharts {
            groupId
            title
            charts {
                ...analyticsChart
            }
        }
    }
    ${AnalyticsChartFragmentDoc}
`;

/**
 * __useGetAnalyticsChartsQuery__
 *
 * To run a query within a React component, call `useGetAnalyticsChartsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetAnalyticsChartsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetAnalyticsChartsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetAnalyticsChartsQuery(
    baseOptions?: Apollo.QueryHookOptions<GetAnalyticsChartsQuery, GetAnalyticsChartsQueryVariables>,
) {
    return Apollo.useQuery<GetAnalyticsChartsQuery, GetAnalyticsChartsQueryVariables>(
        GetAnalyticsChartsDocument,
        baseOptions,
    );
}
export function useGetAnalyticsChartsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetAnalyticsChartsQuery, GetAnalyticsChartsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetAnalyticsChartsQuery, GetAnalyticsChartsQueryVariables>(
        GetAnalyticsChartsDocument,
        baseOptions,
    );
}
export type GetAnalyticsChartsQueryHookResult = ReturnType<typeof useGetAnalyticsChartsQuery>;
export type GetAnalyticsChartsLazyQueryHookResult = ReturnType<typeof useGetAnalyticsChartsLazyQuery>;
export type GetAnalyticsChartsQueryResult = Apollo.QueryResult<
    GetAnalyticsChartsQuery,
    GetAnalyticsChartsQueryVariables
>;
export const GetMetadataAnalyticsChartsDocument = gql`
    query getMetadataAnalyticsCharts($input: MetadataAnalyticsInput!) {
        getMetadataAnalyticsCharts(input: $input) {
            groupId
            title
            charts {
                ...analyticsChart
            }
        }
    }
    ${AnalyticsChartFragmentDoc}
`;

/**
 * __useGetMetadataAnalyticsChartsQuery__
 *
 * To run a query within a React component, call `useGetMetadataAnalyticsChartsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMetadataAnalyticsChartsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMetadataAnalyticsChartsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetMetadataAnalyticsChartsQuery(
    baseOptions: Apollo.QueryHookOptions<GetMetadataAnalyticsChartsQuery, GetMetadataAnalyticsChartsQueryVariables>,
) {
    return Apollo.useQuery<GetMetadataAnalyticsChartsQuery, GetMetadataAnalyticsChartsQueryVariables>(
        GetMetadataAnalyticsChartsDocument,
        baseOptions,
    );
}
export function useGetMetadataAnalyticsChartsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<
        GetMetadataAnalyticsChartsQuery,
        GetMetadataAnalyticsChartsQueryVariables
    >,
) {
    return Apollo.useLazyQuery<GetMetadataAnalyticsChartsQuery, GetMetadataAnalyticsChartsQueryVariables>(
        GetMetadataAnalyticsChartsDocument,
        baseOptions,
    );
}
export type GetMetadataAnalyticsChartsQueryHookResult = ReturnType<typeof useGetMetadataAnalyticsChartsQuery>;
export type GetMetadataAnalyticsChartsLazyQueryHookResult = ReturnType<typeof useGetMetadataAnalyticsChartsLazyQuery>;
export type GetMetadataAnalyticsChartsQueryResult = Apollo.QueryResult<
    GetMetadataAnalyticsChartsQuery,
    GetMetadataAnalyticsChartsQueryVariables
>;
