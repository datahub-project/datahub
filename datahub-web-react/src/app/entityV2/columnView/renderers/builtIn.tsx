import { CheckOutlined } from '@ant-design/icons';
import React from 'react';

import { COLUMN_KIND_SPECS, ColumnKind } from '@app/entityV2/columnView/columnKinds';
import RelationshipChipsCell, { formatTotal } from '@app/entityV2/columnView/renderers/RelationshipChipsCell';
import { DEFAULT_RENDERER_KEY, registerColumnRenderer } from '@app/entityV2/columnView/renderers/registry';
import { RelationshipCell } from '@app/entityV2/columnView/useRelationshipColumnData';

import { DataHubColumnViewColumnType } from '@types';

/**
 * Built-in cell renderers. Importing this module registers them; cells resolve
 * `display.custom.renderer` through the registry and fall back to DEFAULT.
 *
 * Two families of cell feed the registry:
 *  - GRAPH kinds pass their fetched RelationshipCell (chips by DEFAULT).
 *  - The Tags / Glossary Terms attribute kinds pass a synthetic count-only cell (see
 *    `countCell`); they have no registry DEFAULT, so resolveSchemaTableColumns keeps the existing
 *    chip renderer unless CHECK / COUNT is selected.
 */

const isGraph = (type: ColumnKind) => COLUMN_KIND_SPECS[type]?.source === 'GRAPH';
const isMultiGraph = (type: ColumnKind) => isGraph(type) && !COLUMN_KIND_SPECS[type].singleValued;

/** Attribute kinds whose cell is a list of labels; countable, so CHECK / COUNT apply. */
export const COUNTABLE_LABEL_KINDS: ColumnKind[] = [
    DataHubColumnViewColumnType.Tags,
    DataHubColumnViewColumnType.GlossaryTerms,
];
const isCountableLabel = (type: ColumnKind) => COUNTABLE_LABEL_KINDS.includes(type);

/** A RelationshipCell-shaped cell carrying only a count, for non-GRAPH kinds. */
export function countCell(total: number): RelationshipCell {
    return { total, totalIsCapped: false, related: [], count: total };
}

registerColumnRenderer({
    key: DEFAULT_RENDERER_KEY,
    label: 'columnViews.renderer.default',
    appliesTo: isGraph,
    render: (cell, ctx) => <RelationshipChipsCell cell={cell} ctx={ctx} />,
});

/** Check mark when the list is non-empty; the total as a tooltip. `—` otherwise. */
registerColumnRenderer({
    key: 'CHECK',
    label: 'columnViews.renderer.check',
    appliesTo: (type) => isMultiGraph(type) || isCountableLabel(type),
    render: (cell) =>
        cell && cell.total > 0 ? (
            <span title={formatTotal(cell)}>
                <CheckOutlined />
            </span>
        ) : (
            <span>—</span>
        ),
});

/** Just the count (`1,000+` when capped). */
registerColumnRenderer({
    key: 'COUNT',
    label: 'columnViews.renderer.count',
    appliesTo: (type) => isMultiGraph(type) || isCountableLabel(type),
    render: (cell) => <span>{cell && cell.total > 0 ? formatTotal(cell) : '—'}</span>,
});
