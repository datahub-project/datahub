/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import {
    EXECUTOR_TYPE_FIELD,
    INGESTION_SOURCE_FIELD,
    RESULT_STATUS_FIELD,
} from '@app/ingestV2/executions/components/Filters';
import {
    EXECUTOR_TYPE_ALL_VALUE,
    EXECUTOR_TYPE_CLI_VALUE,
} from '@app/ingestV2/shared/components/filters/ExecutorTypeFilter';
import { RESULT_STATUS_ALL_VALUE } from '@app/ingestV2/shared/components/filters/ResultStatusFilter';

export type FacetFilterInput = {
    field: string;
    negated?: boolean;
    values?: string[];
};

interface Response {
    filters: FacetFilterInput[];
    hasAppliedFilters: boolean;
}

export default function useFilters(appliedFilters: Map<string, string[]>): Response {
    return useMemo(() => {
        let hasAppliedFilters = false;
        const filters: FacetFilterInput[] = [];

        const executorType = appliedFilters.get(EXECUTOR_TYPE_FIELD)?.[0];
        if (executorType && executorType !== EXECUTOR_TYPE_ALL_VALUE) {
            filters.push({
                field: 'executorId',
                values: [CLI_EXECUTOR_ID],
                negated: executorType !== EXECUTOR_TYPE_CLI_VALUE,
            });
            hasAppliedFilters = true;
        }

        const sourceUrns = appliedFilters.get(INGESTION_SOURCE_FIELD);
        if (sourceUrns && sourceUrns.length > 0) {
            filters.push({
                field: INGESTION_SOURCE_FIELD,
                values: sourceUrns,
            });
            hasAppliedFilters = true;
        }

        const resultStatus = appliedFilters.get(RESULT_STATUS_FIELD)?.[0];
        if (resultStatus && resultStatus !== RESULT_STATUS_ALL_VALUE) {
            filters.push({
                field: 'executionResultStatus',
                values: [resultStatus],
            });
            hasAppliedFilters = true;
        }

        return { filters, hasAppliedFilters };
    }, [appliedFilters]);
}
