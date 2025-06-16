import { LIST_FORMS_INPUTS } from '@app/govern/Dashboard/Forms/useGetFormsData';
import { GetSearchResultsForMultipleDocument, GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';

import { AssignmentStatus } from '@types';

const addToCache = (existingForms, newForm) => {
    return [newForm, ...existingForms];
};

const addOrUpdateToCache = (existingForms, newForm, formUrn, showPublishing) => {
    const formToWrite = {
        entity: {
            urn: newForm.urn,
            type: 'FORM',
            formInfo: {
                name: newForm.info.name,
                description: newForm.info.description,
                type: newForm.info.type,
                status: newForm.info.status,
                prompts: newForm.info.prompts,
                lastModified: newForm.info.lastModified,
                created: newForm.info.created,
                actors: newForm.info.actors,
            },
            ownership: newForm.ownership,
            dynamicFormAssignment: newForm.dynamicFormAssignment,
            formAssignmentStatus: showPublishing
                ? { status: AssignmentStatus.InProgress, timestamp: new Date().getTime() }
                : newForm.formAssignmentStatus,
            formSettings: newForm.formSettings,
            __typename: 'Form',
        },
        matchedFields: [],
        insights: [],
        extraProperties: [],
        __typename: 'SearchResult',
    };

    const newForms = [...existingForms];
    let didUpdate = false;
    const updatedForms = newForms.map((form) => {
        if (form.entity.urn === formUrn) {
            didUpdate = true;
            return formToWrite;
        }
        return form;
    });
    return didUpdate ? updatedForms : addToCache(existingForms, formToWrite);
};

export const updateFormsList = (client, inputs, newForm, formUrn, searchAcrossEntities, showPublishing) => {
    //  Read the data from our cache for this query.
    const currData: GetSearchResultsForMultipleQuery | null = client.readQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingForms = currData?.searchAcrossEntities?.searchResults || [];
    const newForms = addOrUpdateToCache(existingForms, newForm, formUrn, showPublishing);

    // Write our data back to the cache.
    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: inputs,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newForms.length,
                searchResults: newForms,
            },
        },
    });
};

export const updateFormAssignmentStatusInList = (client, formUrn, formAssignmentStatus) => {
    //  Read the data from our cache for this query.
    const currData: GetSearchResultsForMultipleQuery | null = client.readQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: LIST_FORMS_INPUTS,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const searchAcrossEntities = currData?.searchAcrossEntities || [];
    const existingForms = currData?.searchAcrossEntities?.searchResults || [];
    const formToUpdate = existingForms.map((result) => result.entity as any).find((form) => form.urn === formUrn);
    const updatedForm = { info: formToUpdate?.formInfo, ...formToUpdate, formAssignmentStatus };
    const newForms = addOrUpdateToCache(existingForms, updatedForm, formUrn, false);

    // Write our data back to the cache.
    client.writeQuery({
        query: GetSearchResultsForMultipleDocument,
        variables: {
            input: LIST_FORMS_INPUTS,
        },
        data: {
            searchAcrossEntities: {
                ...searchAcrossEntities,
                total: newForms.length,
                searchResults: newForms,
            },
        },
    });
};
