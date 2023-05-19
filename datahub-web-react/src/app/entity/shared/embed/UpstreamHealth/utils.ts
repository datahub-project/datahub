import { Dataset, EntityType, FilterOperator, LineageDirection } from '../../../../../types.generated';

export function getNumAssertionsFailing(dataset: Dataset) {
    let numFailing = 0;

    dataset.assertions?.assertions.forEach((assertion) => {
        if (assertion.runEvents?.failed) {
            numFailing += 1;
        }
    });

    return numFailing;
}

export const DATASET_COUNT = 5;

export function generateQueryVariables(
    urn: string,
    filterField: string,
    start: number,
    includeAssertions: boolean,
    skip?: boolean,
    count?: number,
) {
    return {
        skip,
        variables: {
            input: {
                urn,
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
        },
    };
}
