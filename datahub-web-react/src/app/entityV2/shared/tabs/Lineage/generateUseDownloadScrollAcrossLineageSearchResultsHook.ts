/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '@app/search/utils/types';

import { useGetDownloadScrollAcrossLineageResultsQuery } from '@graphql/scroll.generated';
import { LineageDirection } from '@types';

/**
 * Generates a hook which can be used to download Scroll Across Lineage Results to CSV inside the
 * Download as CSV modal.
 */
export default function generateUseDownloadSearchAcrossLineageSearchResultsHook({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
}: {
    urn: string;
    direction: LineageDirection;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
}) {
    return function useDownloadSearchAcrossLineageSearchResults(params: DownloadSearchResultsParams) {
        const { data, loading, error, refetch } = useGetDownloadScrollAcrossLineageResultsQuery({
            ...params,
            variables: {
                input: {
                    ...params.variables.input,
                    urn,
                    direction,
                    startTimeMillis: startTimeMillis || undefined,
                    endTimeMillis: endTimeMillis || undefined,
                },
            },
        });

        return {
            searchResults: (data?.scrollAcrossLineage && {
                ...data?.scrollAcrossLineage,
                nextScrollId: data?.scrollAcrossLineage?.nextScrollId,
                searchResults: data?.scrollAcrossLineage?.searchResults,
            }) as DownloadSearchResults,
            loading,
            error,
            refetch: (input: DownloadSearchResultsInput) => {
                return refetch({
                    input: {
                        ...input,
                        urn,
                        direction,
                        startTimeMillis: startTimeMillis || undefined,
                        endTimeMillis: endTimeMillis || undefined,
                    },
                }).then((res) => {
                    return (res.data?.scrollAcrossLineage && {
                        ...res.data?.scrollAcrossLineage,
                        nextScrollId: res.data?.scrollAcrossLineage?.nextScrollId,
                    }) as DownloadSearchResults;
                });
            },
        };
    };
}
