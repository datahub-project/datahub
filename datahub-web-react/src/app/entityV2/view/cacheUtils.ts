import {
    ListGlobalViewsDocument,
    ListGlobalViewsQuery,
    ListMyViewsDocument,
    ListMyViewsQuery,
} from '../../../graphql/view.generated';
import { DataHubViewType, DataHubView } from '../../../types.generated';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from './utils';

/**
 * This file contains utility classes for manipulating the Apollo Cache
 * when Views are added, updated, or removed.
 */

const addViewToList = (existingViews, newView) => {
    return [newView, ...existingViews];
};

const addOrUpdateViewInList = (existingViews, newView) => {
    const newViews = [...existingViews];
    let didUpdate = false;
    const updatedViews = newViews.map((view) => {
        if (view.urn === newView.urn) {
            didUpdate = true;
            return newView;
        }
        return view;
    });
    return didUpdate ? updatedViews : addViewToList(existingViews, newView);
};

export const updateListMyViewsCache = (
    urn: string,
    newView: DataHubView,
    client,
    page,
    pageSize,
    viewType?: DataHubViewType,
    query?: string,
) => {
    // Read the data from our cache for this query.
    const currData: ListMyViewsQuery | null = client.readQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            viewType,
            query,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingViews = currData?.listMyViews?.views || [];
    const newViews = addOrUpdateViewInList(existingViews, newView);

    const currCount = currData?.listMyViews?.count || 0;
    const currTotal = currData?.listMyViews?.total || 0;
    const didAdd = newViews.length > existingViews.length;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListMyViewsDocument,
        variables: {
            viewType,
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listMyViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: currCount + (didAdd ? 1 : 0),
                total: currTotal + (didAdd ? 1 : 0),
                views: newViews,
            },
        },
    });
};

const updateListGlobalViewsCache = (urn: string, newView: DataHubView, client, page, pageSize, query?: string) => {
    // Read the data from our cache for this query.
    const currData: ListGlobalViewsQuery | null = client.readQuery({
        query: ListGlobalViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingViews = currData?.listGlobalViews?.views || [];
    const newViews = addOrUpdateViewInList(existingViews, newView);
    const didAdd = newViews.length > existingViews.length;

    const currCount = currData?.listGlobalViews?.count || 0;
    const currTotal = currData?.listGlobalViews?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListGlobalViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listGlobalViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: currCount + (didAdd ? 1 : 0),
                total: currTotal + (didAdd ? 1 : 0),
                views: newViews,
            },
        },
    });
};

export const removeFromListMyViewsCache = (
    urn: string,
    client,
    page: number,
    pageSize: number,
    viewType?: DataHubViewType,
    query?: string,
) => {
    const currData: ListMyViewsQuery | null = client.readQuery({
        query: ListMyViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            viewType,
            query,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingViews = currData?.listMyViews?.views || [];
    const newViews = [...existingViews.filter((view) => view.urn !== urn)];
    const didRemove = existingViews.length !== newViews.length;

    const currCount = currData?.listMyViews?.count || 0;
    const currTotal = currData?.listMyViews?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListMyViewsDocument,
        variables: {
            viewType,
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listMyViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: didRemove ? currCount - 1 : currCount,
                total: didRemove ? currTotal - 1 : currCount,
                views: newViews,
            },
        },
    });
};

const removeFromListGlobalViewsCache = (urn: string, client, page: number, pageSize: number, query?: string) => {
    const currData: ListGlobalViewsQuery | null = client.readQuery({
        query: ListGlobalViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingViews = currData?.listGlobalViews?.views || [];
    const newViews = [...existingViews.filter((view) => view.urn !== urn)];
    const didRemove = existingViews.length !== newViews.length;

    const currCount = currData?.listGlobalViews?.count || 0;
    const currTotal = currData?.listGlobalViews?.total || 0;

    // Write our data back to the cache.
    client.writeQuery({
        query: ListGlobalViewsDocument,
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query,
        },
        data: {
            listGlobalViews: {
                __typename: 'ListViewsResult',
                start: (page - 1) * pageSize,
                count: didRemove ? currCount - 1 : currCount,
                total: didRemove ? currTotal - 1 : currCount,
                views: newViews,
            },
        },
    });
};

/**
 * Updates an entry in the list views caches, which are used
 * to power the View Select component.
 *
 * @param urn the urn of the updated view
 * @param view the updated view
 * @param client the Apollo client
 */
export const updateViewSelectCache = (urn: string, view: DataHubView, client) => {
    if (view.viewType === DataHubViewType.Personal) {
        // Add or Update in Personal Views Cache, remove from the opposite.
        updateListMyViewsCache(urn, view, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, DataHubViewType.Personal, undefined);
        removeFromListGlobalViewsCache(urn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined);
    } else {
        // Add or Update in Global Views Cache, remove from the opposite.
        updateListGlobalViewsCache(urn, view, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined);
        removeFromListMyViewsCache(urn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, DataHubViewType.Personal, undefined);
    }
};

/**
 * Removes an URN from the queries used to power the View Select dropdown.
 *
 * @param urn the urn of the updated view
 * @param client the Apollo client
 */
export const removeFromViewSelectCaches = (urn: string, client) => {
    removeFromListMyViewsCache(urn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, DataHubViewType.Personal, undefined);
    removeFromListGlobalViewsCache(urn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined);
};
