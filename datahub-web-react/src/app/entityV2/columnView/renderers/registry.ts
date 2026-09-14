import React from 'react';

import { ColumnKind, ColumnLike } from '@app/entityV2/columnView/columnKinds';
import { ColumnDisplayLike } from '@app/entityV2/columnView/types';
import { RelationshipCell } from '@app/entityV2/columnView/useRelationshipColumnData';

import { DataHubColumnViewColumnType, EntityType } from '@types';

/** Everything a cell renderer may need; no data fetching happens inside renderers. */
export interface ColumnRendererContext {
    column: ColumnLike;
    /** Effective display (spec default merged under the stored one). */
    display: ColumnDisplayLike;
    t: (key: string, opts?: any) => string;
    entityUrl: (type: EntityType, urn: string) => string;
    /** Popover pagination: request a larger page for this cell (server-capped). */
    loadMore?: (count: number) => Promise<RelationshipCell | undefined>;
    /** Server-side max items per cell; `Load more` is hidden once reached. */
    limit: number;
}

export interface ColumnRenderer {
    /** Stored in `display.custom.renderer`. */
    key: string;
    /** i18n key for the gear "Format" dropdown. */
    label: string;
    appliesTo: (type: ColumnKind) => boolean;
    /** Extra option keys the renderer reads from `display.custom` (documentation for the gear UI). */
    options?: string[];
    render: (cell: RelationshipCell | undefined, ctx: ColumnRendererContext) => React.ReactNode;
}

const RENDERERS = new Map<string, ColumnRenderer>();

export const DEFAULT_RENDERER_KEY = 'DEFAULT';

export function registerColumnRenderer(renderer: ColumnRenderer) {
    RENDERERS.set(renderer.key, renderer);
}

export function getRenderersFor(type: DataHubColumnViewColumnType): ColumnRenderer[] {
    return Array.from(RENDERERS.values()).filter((r) => r.appliesTo(type));
}

/** The renderer selected by `display.custom.renderer`, if registered and applicable; else DEFAULT. */
export function resolveRenderer(type: DataHubColumnViewColumnType, key?: string): ColumnRenderer | undefined {
    const wanted = key ? RENDERERS.get(key) : undefined;
    if (wanted && wanted.appliesTo(type)) return wanted;
    const fallback = RENDERERS.get(DEFAULT_RENDERER_KEY);
    return fallback && fallback.appliesTo(type) ? fallback : undefined;
}
