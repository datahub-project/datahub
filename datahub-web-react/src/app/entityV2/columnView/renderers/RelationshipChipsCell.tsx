import { Button, Popover } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { COLUMN_KIND_SPECS } from '@app/entityV2/columnView/columnKinds';
import { ColumnRendererContext } from '@app/entityV2/columnView/renderers/registry';
import { RelatedItem, RelationshipCell } from '@app/entityV2/columnView/useRelationshipColumnData';
import OverflowList from '@app/sharedV2/OverflowList';

import { DataHubColumnViewExpand, DataHubColumnViewLabelStyle, DataHubColumnViewOverflow } from '@types';

const Chip = styled.a`
    display: inline-block;
    max-width: 100%;
    padding: 0 6px;
    border-radius: 6px;
    background: ${(p) => p.theme.colors.bgSurface};
    font-size: 12px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const CountChip = styled.span`
    padding: 0 6px;
    border-radius: 6px;
    background: ${(p) => p.theme.colors.bgSurface};
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
`;

const Inline = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const PopoverList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-height: 320px;
    overflow-y: auto;
    min-width: 240px;
`;

export const CAPPED_TOTAL = 1000;

const lastSegment = (name?: string | null) => (name ? name.split('.').pop() || name : '');

/** Label for one related item under a labelStyle. `others` drives FIELD_NAME disambiguation. */
export function labelFor(item: RelatedItem, style: DataHubColumnViewLabelStyle | null | undefined, others: RelatedItem[]): string {
    const field = item.fieldPath || lastSegment(item.datasetName) || item.urn;
    if (!item.fieldPath) return item.datasetName || item.urn;
    switch (style) {
        case DataHubColumnViewLabelStyle.FullPath:
            return item.datasetName ? `${item.datasetName}.${item.fieldPath}` : item.fieldPath;
        case DataHubColumnViewLabelStyle.DatasetAndField:
            return item.datasetName ? `${lastSegment(item.datasetName)}.${item.fieldPath}` : item.fieldPath;
        case DataHubColumnViewLabelStyle.FieldName:
        default: {
            // Shortest-unique suffix: bare field name unless another item in the cell shares it.
            const clash = others.some((o) => o.urn !== item.urn && o.fieldPath === item.fieldPath);
            return clash && item.datasetName ? `${lastSegment(item.datasetName)}.${field}` : field;
        }
    }
}

/** Full `datasetName.fieldPath` for popovers, regardless of labelStyle. */
export const fullLabelFor = (item: RelatedItem) =>
    item.fieldPath ? `${item.datasetName || item.datasetUrn || ''}${item.datasetName || item.datasetUrn ? '.' : ''}${item.fieldPath}` : item.datasetName || item.urn;

export function formatTotal(cell: RelationshipCell): string {
    return cell.totalIsCapped ? `${CAPPED_TOTAL.toLocaleString()}+` : cell.total.toLocaleString();
}

function ItemLink({ item, label, ctx }: { item: RelatedItem; label: string; ctx: ColumnRendererContext }) {
    return (
        <Chip href={ctx.entityUrl(item.entityType, item.urn)} title={fullLabelFor(item)}>
            {label}
        </Chip>
    );
}

function HiddenItemsPopover({ cell, ctx, hiddenCount }: { cell: RelationshipCell; ctx: ColumnRendererContext; hiddenCount: number }) {
    const [loading, setLoading] = useState(false);
    const canLoadMore = cell.related.length < cell.total && cell.count < ctx.limit && Boolean(ctx.loadMore);
    const marker = ctx.display.overflow === DataHubColumnViewOverflow.Ellipsis ? '…' : `+${hiddenCount}`;
    const content = (
        <PopoverList>
            {cell.related.map((item) => (
                <ItemLink key={item.urn} item={item} label={fullLabelFor(item)} ctx={ctx} />
            ))}
            {cell.related.length < cell.total && (
                <span>
                    {ctx.t('columnViews.relationship.showingOf', { shown: cell.related.length, total: formatTotal(cell) })}
                </span>
            )}
            {canLoadMore && (
                <Button
                    size="small"
                    type="link"
                    loading={loading}
                    onClick={() => {
                        setLoading(true);
                        ctx.loadMore?.(Math.min(ctx.limit, cell.count * 2)).finally(() => setLoading(false));
                    }}
                >
                    {ctx.t('columnViews.relationship.loadMore')}
                </Button>
            )}
        </PopoverList>
    );
    return (
        <Popover content={content} trigger="click" placement="bottomLeft">
            <CountChip title={formatTotal(cell)}>{marker}</CountChip>
        </Popover>
    );
}

/**
 * Default GRAPH cell: what fits, then `+N` / `…` opening a paginated popover (or inline expansion
 * per display.expand). `—` when empty; single-valued kinds render one chip without a count.
 */
export default function RelationshipChipsCell({ cell, ctx }: { cell: RelationshipCell | undefined; ctx: ColumnRendererContext }) {
    const [expanded, setExpanded] = useState(false);
    const spec = COLUMN_KIND_SPECS[ctx.column.type];
    const items = useMemo(
        () =>
            (cell?.related || []).map((item) => ({
                key: item.urn,
                item,
                node: <ItemLink item={item} label={labelFor(item, ctx.display.labelStyle, cell?.related || [])} ctx={ctx} />,
            })),
        [cell, ctx],
    );

    if (!cell || cell.related.length === 0) return <span>—</span>;

    if (spec.singleValued) {
        return items[0].node;
    }

    const beyondPage = Math.max(0, cell.total - cell.related.length);

    if (ctx.display.expand === DataHubColumnViewExpand.Inline && expanded) {
        return (
            <Inline>
                {items.map((i) => (
                    <React.Fragment key={i.key}>{i.node}</React.Fragment>
                ))}
                {beyondPage > 0 && <HiddenItemsPopover cell={cell} ctx={ctx} hiddenCount={beyondPage} />}
            </Inline>
        );
    }

    return (
        <OverflowList
            items={items}
            gap={4}
            renderHiddenItems={(hidden) => {
                const hiddenCount = hidden.length + beyondPage;
                if (hiddenCount === 0) return null;
                if (ctx.display.expand === DataHubColumnViewExpand.Inline) {
                    return (
                        <CountChip onClick={() => setExpanded(true)}>
                            {ctx.display.overflow === DataHubColumnViewOverflow.Ellipsis ? '…' : `+${hiddenCount}`}
                        </CountChip>
                    );
                }
                return <HiddenItemsPopover cell={cell} ctx={ctx} hiddenCount={hiddenCount} />;
            }}
        />
    );
}
