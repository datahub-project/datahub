import { ListMyViewsDocument, ListMyViewsQuery } from '../../../graphql/view.generated';
import { EntityType } from '../../../types.generated';
import { ViewBuilderState } from './types';

/**
 * Converts an instance of the View builder state
 * into the input required to create or update a View in
 * GraphQL.
 *
 * @param state the builder state
 */
export const convertStateToUpdateInput = (state: ViewBuilderState) => {
    return {
        viewType: state.viewType,
        name: state.name as string,
        description: state.description as string,
        definition: {
            entityTypes: state?.definition?.entityTypes,
            filter: {
                operator: state?.definition?.filter?.operator,
                filters: state?.definition?.filter?.filters.map((filter) => ({
                    field: filter.field,
                    condition: filter.condition,
                    values: filter.values,
                    negated: filter.negated,
                })),
            },
        },
    };
};

const updateViewInList = (existingViews, newView) => {
    const newViews = [...existingViews];
    return newViews.map((view) => {
        if (view.urn === newView.urn) {
            return newView;
        }
        return view;
    });
};

const addViewToList = (existingViews, newView) => {
    return [newView, ...existingViews];
};

/**
 * Adds or updates an entry in the list views cache.
 *
 * @param urn the urn of the updated view
 * @param state the updated view state
 * @param client the Apollo client
 * @param page the current page of results.
 * @param pageSize the size of pages.
 * @param exists whether the URN already exists in the result list.
 */
export const updateListViewsCache = (
    urn: string,
    state: ViewBuilderState,
    client,
    page: number,
    pageSize: number,
    exists: boolean,
    query?: string,
) => {
    // Read the data from our cache for this query.
    const currData: ListMyViewsQuery | null = client.readQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
    });

    const newView = {
        urn,
        type: EntityType.DatahubView,
        name: state.name,
        description: state.description,
        viewType: state.viewType,
        definition: state.definition,
    };

    const existingViews = currData?.listMyViews?.views || [];
    const newViews = exists ? updateViewInList(existingViews, newView) : addViewToList(existingViews, newView);

    const currCount = currData?.listMyViews?.count || 0;
    const currTotal = currData?.listMyViews?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listMyViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: currCount + (exists ? 0 : 1),
                total: currTotal + (exists ? 0 : 1),
                views: newViews,
            },
        },
    });
};

/**
 * Removes an URN from the List Views cache
 *
 * @param urn the urn of the updated view
 * @param client the Apollo client
 * @param page the current page of results.
 * @param pageSize the size of pages.
 * @param query the query that is currently typed, or undefined if there is none.
 */
export const removeFromListViewsCache = (urn: string, client, page: number, pageSize: number, query?: string) => {
    const currData: ListMyViewsQuery | null = client.readQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
    });

    const existingViews = currData?.listMyViews?.views || [];
    const newViews = [...existingViews.filter((view) => view.urn !== urn)];

    const currCount = currData?.listMyViews?.count || 0;
    const currTotal = currData?.listMyViews?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listMyViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: currCount - 1,
                total: currTotal - 1,
                views: newViews,
            },
        },
    });
};
