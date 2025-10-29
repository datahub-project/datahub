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
