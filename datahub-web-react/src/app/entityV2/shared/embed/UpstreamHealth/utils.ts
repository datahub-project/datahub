import { Dataset } from '@types';

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
