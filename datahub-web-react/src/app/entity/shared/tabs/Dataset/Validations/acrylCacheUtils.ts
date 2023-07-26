import {
    GetDatasetAssertionsWithMonitorsDocument,
    GetDatasetAssertionsWithMonitorsQuery,
} from '../../../../../../graphql/monitor.generated';
import { Monitor, Assertion } from '../../../../../../types.generated';

/**
 * This file contains utility classes for manipulating the Apollo Cache
 * when assertions are added, updated, or removed.
 */

const addAssertionToList = (existingAssertions, newAssertion) => {
    return [...existingAssertions, newAssertion];
};

const addOrUpdateAssertionInList = (existingAssertions, newAssertion) => {
    const newAssertions = [...existingAssertions];
    let didUpdate = false;
    const updatedAssertions = newAssertions.map((assertion) => {
        if (assertion.urn === newAssertion.urn) {
            didUpdate = true;
            return {
                ...assertion,
                ...newAssertion,
            };
        }
        return assertion;
    });
    return didUpdate ? updatedAssertions : addAssertionToList(existingAssertions, newAssertion);
};

/**
 * Add or update an assertion in the dataset assertions cache.
 *
 * Note that this expects that any monitor objects are already present on the assertion under the "monitor" field,
 * as selected in the assertionsQueryWithMonitors fragment in monitor.graphql. Also expects that the "runEvents"
 * field is present on the assertion.
 *
 * If either of these fields are missing in the assertion object, the cache WILL NOT work. This is due to how
 * Apollo cache requires a value for all projected fields in order to find a cache hit.
 */
export const updateDatasetAssertionsCache = (datasetUrn: string, newAssertion: any, client) => {
    // Read the data from our cache for this query.
    const currData: GetDatasetAssertionsWithMonitorsQuery | null = client.readQuery({
        query: GetDatasetAssertionsWithMonitorsDocument,
        variables: {
            urn: datasetUrn,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingAssertions = currData?.dataset?.assertions?.assertions || [];
    const newAssertions = addOrUpdateAssertionInList(existingAssertions, newAssertion);
    const didAdd = newAssertions.length > existingAssertions.length;

    const currCount = currData?.dataset?.assertions?.count || 0;
    const currTotal = currData?.dataset?.assertions?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: GetDatasetAssertionsWithMonitorsDocument,
        variables: {
            urn: datasetUrn,
        },
        data: {
            dataset: {
                ...currData?.dataset,
                assertions: {
                    start: 0,
                    count: currCount + (didAdd ? 1 : 0),
                    total: currTotal + (didAdd ? 1 : 0),
                    assertions: newAssertions,
                },
            },
        },
    });
};

/**
 * Remove an assertion from the dataset assertions cache, e.g. when the assertion is removed.
 */
export const removeFromDatasetAssertionsCache = (urn: string, assertionUrn: string, client) => {
    const currData: GetDatasetAssertionsWithMonitorsQuery | null = client.readQuery({
        query: GetDatasetAssertionsWithMonitorsDocument,
        variables: {
            urn,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingAssertions = currData?.dataset?.assertions?.assertions || [];
    const newAssertions = [...existingAssertions.filter((assertion) => assertion.urn !== assertionUrn)];
    const didRemove = existingAssertions.length !== newAssertions.length;

    const currCount = currData?.dataset?.assertions?.count || 0;
    const currTotal = currData?.dataset?.assertions?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: GetDatasetAssertionsWithMonitorsDocument,
        variables: {
            urn,
        },
        data: {
            dataset: {
                ...currData?.dataset,
                assertions: {
                    start: 0,
                    count: didRemove ? currCount - 1 : currCount,
                    total: didRemove ? currTotal - 1 : currCount,
                    assertions: newAssertions,
                },
            },
        },
    });
};

/**
 * Creates an assertion in the shape returned by the getDatasetAssertions GraphQL query, which
 * includes the monitor embedded inside the assertion.
 */
export const createCachedAssertionWithMonitor = (assertion: Assertion, monitor: Monitor) => {
    return {
        ...assertion,
        monitor: {
            start: 0,
            count: 1,
            total: 1,
            relationships: [
                {
                    entity: monitor,
                },
            ],
        },
        runEvents: {
            total: 0,
            failed: 0,
            succeeded: 0,
            runEvents: [],
        },
    };
};
