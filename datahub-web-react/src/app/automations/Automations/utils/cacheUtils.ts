import { ActionPipelineState } from '@src/types.generated';

import {
    GetActionPipelineStatusDocument,
    GetActionPipelineStatusQuery,
    ListActionPipelinesDocument,
    ListActionPipelinesQuery,
} from '@graphql/actionPipeline.generated';

const addOrUpdateAutomationInList = (existingAutomations, newAutomation) => {
    const newAutomations = [...existingAutomations];
    let didUpdate = false;
    const updatedAutomations = newAutomations.map((automation) => {
        if (automation.urn === newAutomation.urn) {
            didUpdate = true;
            return newAutomation;
        }
        return automation;
    });
    return didUpdate ? updatedAutomations : [newAutomation, ...existingAutomations];
};

/**
 * Add an entry to the ListAutomations cache.
 */
export const updateListAutomationsCache = (client, newAutomation, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListActionPipelinesQuery | null = client.readQuery({
        query: ListActionPipelinesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    // Add our new automation into the existing list.
    const existingAutomations = [...(currData?.listActionPipelines?.actionPipelines || [])];
    const newAutomations = addOrUpdateAutomationInList(existingAutomations, {
        ...newAutomation,
        __typename: 'ActionPipeline',
    });
    const didAddAutomation = newAutomations.length > existingAutomations.length;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListActionPipelinesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listActionPipelines: {
                start: 0,
                count: pageSize,
                total: didAddAutomation
                    ? (currData?.listActionPipelines?.total || 0) + 1
                    : currData?.listActionPipelines?.total,
                actionPipelines: newAutomations,
                __typename: 'ListActionPipelinesResult',
            },
        },
    });
};

/**
 * Remove an entry from the ListAutomations cache.
 */
export const removeFromListAutomationsCache = (client, urn, pageSize) => {
    // Read the data from our cache for this query.
    const currData: ListActionPipelinesQuery | null = client.readQuery({
        query: ListActionPipelinesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    // Remove the automation from the existing automations set.
    const newAutomations = [
        ...(currData?.listActionPipelines?.actionPipelines || []).filter((automation) => automation.urn !== urn),
    ];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListActionPipelinesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listActionPipelines: {
                start: currData?.listActionPipelines?.start || 0,
                count: (currData?.listActionPipelines?.count || 1) - 1,
                total: (currData?.listActionPipelines?.total || 1) - 1,
                actionPipelines: newAutomations,
            },
        },
    });

    console.log(`deleted from cache~`);
};

/**
 * Add an entry to the GetActionPipelineStatus cache.
 */
export const updateGetActionPipelineStatusCache = (client, urn: string, newState: ActionPipelineState) => {
    // Read the data from our cache for this query.
    const currData: GetActionPipelineStatusQuery | null = client.readQuery({
        query: GetActionPipelineStatusDocument,
        variables: { urn },
    });

    console.log(`curr data in cache`, currData);

    // Write our data back to the cache.
    client.writeQuery({
        query: GetActionPipelineStatusDocument,
        variables: { urn },
        data: {
            actionPipeline: {
                ...currData?.actionPipeline,
                details: {
                    ...currData?.actionPipeline?.details,
                    state: newState,
                },
            },
        },
    });
};
