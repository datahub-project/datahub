import { ListTestsDocument, ListTestsQuery } from '../../graphql/test.generated';

const addOrUpdateTestInList = (existingTests, newTest) => {
    const newTests = [...existingTests];
    let didUpdate = false;
    const updatedTests = newTests.map((test) => {
        if (test.urn === newTest.urn) {
            didUpdate = true;
            return newTest;
        }
        return test;
    });
    return didUpdate ? updatedTests : [newTest, ...existingTests];
};

/**
 * Add an entry to the ListTests cache.
 */
export const updateListTestsCache = (client, newTest, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListTestsQuery | null = client.readQuery({
        query: ListTestsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query: undefined,
            },
        },
    });

    // Add our new test into the existing list.
    const existingTests = [...(currData?.listTests?.tests || [])];
    const newTests = addOrUpdateTestInList(existingTests, newTest);
    const didAddTest = newTests.length > existingTests.length;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListTestsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query: undefined,
            },
        },
        data: {
            listTests: {
                __typename: 'ListTestsResult',
                start: 0,
                count: didAddTest ? (currData?.listTests?.count || 0) + 1 : currData?.listTests?.count,
                total: didAddTest ? (currData?.listTests?.total || 0) + 1 : currData?.listTests?.total,
                tests: newTests,
            },
        },
    });
};

/**
 * Remove an entry from the ListTests cache.
 */
export const removeFromListTestsCache = (client, urn, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListTestsQuery | null = client.readQuery({
        query: ListTestsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    // Remove the test from the existing tests set.
    const newTests = [...(currData?.listTests?.tests || []).filter((test) => test.urn !== urn)];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListTestsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listTests: {
                start: currData?.listTests?.start || 0,
                count: (currData?.listTests?.count || 1) - 1,
                total: (currData?.listTests?.total || 1) - 1,
                tests: newTests,
            },
        },
    });
};
