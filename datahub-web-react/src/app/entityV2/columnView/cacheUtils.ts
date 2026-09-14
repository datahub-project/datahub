import { DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, SCHEMA_TARGET } from '@app/entityV2/columnView/types';

import {
    ListGlobalColumnViewsDocument,
    ListGlobalColumnViewsQuery,
    ListMyColumnViewsDocument,
    ListMyColumnViewsQuery,
} from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubViewType } from '@types';

/**
 * Thin parameterized copy of app/entityV2/view/cacheUtils.ts for the Column View select caches.
 * Kept separate on purpose: the Views cache utils are keyed on `views` / ListViewsResult and this
 * one on `columnViews` / ListColumnViewsResult, and the query variables differ (target).
 */

type ListKey = 'listMyColumnViews' | 'listGlobalColumnViews';

const upsert = (existing: DataHubColumnView[], next: DataHubColumnView) => {
    const idx = existing.findIndex((v) => v.urn === next.urn);
    if (idx === -1) return [next, ...existing];
    const copy = [...existing];
    copy[idx] = next;
    return copy;
};

function rewrite(
    client,
    key: ListKey,
    document,
    variables,
    mutate: (views: DataHubColumnView[]) => DataHubColumnView[],
) {
    const curr: ListMyColumnViewsQuery | ListGlobalColumnViewsQuery | null = client.readQuery({
        query: document,
        variables,
    });
    if (curr === null) return; // first load has not happened; let it occur naturally
    const existing = (curr as any)?.[key]?.columnViews || [];
    const next = mutate(existing);
    const delta = next.length - existing.length;
    client.writeQuery({
        query: document,
        variables,
        data: {
            [key]: {
                __typename: 'ListColumnViewsResult',
                start: variables.start,
                count: ((curr as any)?.[key]?.count || 0) + delta,
                total: ((curr as any)?.[key]?.total || 0) + delta,
                columnViews: next,
            },
        },
    });
}

const myVars = { start: 0, count: DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, viewType: DataHubViewType.Personal, target: SCHEMA_TARGET };
const globalVars = { start: 0, count: DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, target: SCHEMA_TARGET };

export const updateColumnViewSelectCache = (urn: string, view: DataHubColumnView, client) => {
    const remove = (views: DataHubColumnView[]) => views.filter((v) => v.urn !== urn);
    if (view.viewType === DataHubViewType.Personal) {
        rewrite(client, 'listMyColumnViews', ListMyColumnViewsDocument, myVars, (v) => upsert(v, view));
        rewrite(client, 'listGlobalColumnViews', ListGlobalColumnViewsDocument, globalVars, remove);
    } else {
        rewrite(client, 'listGlobalColumnViews', ListGlobalColumnViewsDocument, globalVars, (v) => upsert(v, view));
        rewrite(client, 'listMyColumnViews', ListMyColumnViewsDocument, myVars, remove);
    }
};

export const removeFromColumnViewSelectCaches = (urn: string, client) => {
    const remove = (views: DataHubColumnView[]) => views.filter((v) => v.urn !== urn);
    rewrite(client, 'listMyColumnViews', ListMyColumnViewsDocument, myVars, remove);
    rewrite(client, 'listGlobalColumnViews', ListGlobalColumnViewsDocument, globalVars, remove);
};
