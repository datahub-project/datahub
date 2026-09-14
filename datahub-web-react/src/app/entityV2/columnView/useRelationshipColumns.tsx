import { ColumnsType } from 'antd/es/table';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import '@app/entityV2/columnView/renderers/builtIn';
import {
    ColumnLike,
    columnHeader,
    columnIdentity,
    columnWidth,
    customDisplayValue,
    displayFor,
} from '@app/entityV2/columnView/columnKinds';
import { ColumnRendererContext, resolveRenderer } from '@app/entityV2/columnView/renderers/registry';
import { RENDERER_CUSTOM_KEY } from '@app/entityV2/columnView/types';
import { useRelationshipColumnData } from '@app/entityV2/columnView/useRelationshipColumnData';
import { VisibleRows } from '@app/entityV2/columnView/useVisibleRowUrns';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

type Column = ColumnsType<ExtendedSchemaFields>[number];

/** Zero-size probe the IntersectionObserver watches; one per row, in the first GRAPH cell. */
const Probe = React.memo(({ refCb }: { refCb: (el: Element | null) => void }) => (
    <span ref={refCb} style={{ display: 'inline-block', width: 0, height: 0 }} />
));

/**
 * Builds antd columns for GRAPH (relationship) kinds, keyed by column identity. Data is fetched
 * for VISIBLE rows only, outside the main schema query, under the caller's context, and cached
 * per (kind, fieldUrn). Cells render through the renderer registry (display.custom.renderer).
 */
export function useRelationshipColumns(
    graphColumns: ColumnLike[],
    visibleRows: VisibleRows,
    opts: { columnViewUrn?: string } = {},
): Record<string, Column> {
    const { t } = useTranslation('entity.views');
    const registry = useEntityRegistry();
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const { data, loadMore, limit } = useRelationshipColumnData(visibleRows.visibleUrns, graphColumns, opts);

    return useMemo(() => {
        const result: Record<string, Column> = {};
        graphColumns.forEach((col, index) => {
            const id = columnIdentity(col);
            const byField = data.get(col.type);
            const display = displayFor(col);
            const renderer = resolveRenderer(col.type, customDisplayValue(col, RENDERER_CUSTOM_KEY));
            const header = columnHeader(col, t, { logicalModelsEnabled });
            result[id] = {
                key: id,
                width: columnWidth(col),
                ellipsis: { showTitle: true },
                title: <span title={header}>{header}</span>,
                render: (_: unknown, record: any) => {
                    const fieldUrn: string | undefined = record.schemaFieldEntity?.urn;
                    if (!fieldUrn) return <span>—</span>;
                    const ctx: ColumnRendererContext = {
                        column: col,
                        display,
                        t: (key, opts) => t(key, opts) as string,
                        entityUrl: (type, urn) => registry.getEntityUrl(type, urn),
                        loadMore: (count) => loadMore(col.type, fieldUrn, count),
                        limit,
                    };
                    return (
                        <>
                            {index === 0 && <Probe refCb={visibleRows.probeRef(fieldUrn)} />}
                            {renderer ? renderer.render(byField?.get(fieldUrn), ctx) : <span>—</span>}
                        </>
                    );
                },
            };
        });
        return result;
    }, [graphColumns, data, t, logicalModelsEnabled, registry, loadMore, limit, visibleRows]);
}
