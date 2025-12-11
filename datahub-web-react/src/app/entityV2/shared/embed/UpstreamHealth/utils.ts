/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Dataset, EntityType, FilterOperator, LineageDirection } from '@types';

export function getNumAssertionsFailing(dataset: Dataset) {
    let numFailing = 0;

    dataset.assertions?.assertions?.forEach((assertion) => {
        if (assertion.runEvents?.failed) {
            numFailing += 1;
        }
    });

    return numFailing;
}

export const DATASET_COUNT = 5;

interface Arguments {
    urn: string;
    filterField: string;
    start: number;
    includeAssertions: boolean;
    includeIncidents: boolean;
    startTimeMillis: number | null;
    skip?: boolean;
    count?: number;
}

export function generateQueryVariables({
    urn,
    startTimeMillis,
    filterField,
    start,
    includeAssertions,
    includeIncidents,
    skip,
    count,
}: Arguments) {
    return {
        skip,
        variables: {
            input: {
                urn,
                startTimeMillis,
                query: '*',
                types: [EntityType.Dataset],
                start,
                count: count !== undefined ? count : DATASET_COUNT,
                direction: LineageDirection.Upstream,
                orFilters: [
                    {
                        and: [
                            { field: 'degree', condition: FilterOperator.Equal, values: ['1', '2', '3+'] },
                            { field: filterField, condition: FilterOperator.Equal, values: ['true'] },
                        ],
                    },
                ],
            },
            includeAssertions,
            includeIncidents,
        },
    };
}
