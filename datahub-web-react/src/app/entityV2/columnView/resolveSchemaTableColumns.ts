import { ColumnsType } from 'antd/es/table';

import { HeaderResizeProps } from '@app/entityV2/columnView/ResizableHeaderCell';
import {
    COLUMN_KIND_SPECS,
    ColumnLike,
    columnIdentity,
    columnTableKey,
    columnWidth,
    customDisplayValue,
    displayFor,
    isLabelColumn,
    isRelationshipColumn,
    isStructuredPropertyColumn,
} from '@app/entityV2/columnView/columnKinds';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { countCell } from '@app/entityV2/columnView/renderers/builtIn';
import { ColumnRendererContext, DEFAULT_RENDERER_KEY, resolveRenderer } from '@app/entityV2/columnView/renderers/registry';
import { RENDERER_CUSTOM_KEY } from '@app/entityV2/columnView/types';

import { DataHubColumnViewColumnType, DataHubColumnViewDefinition } from '@types';

type Column = ColumnsType<ExtendedSchemaFields>[number];

export interface SchemaTableColumnSources {
    /** The implicit Name column, always first. */
    fieldColumn: Column;
    /** Built columns for attribute kinds, keyed by kind. Missing = feature-flagged off. */
    byKind: Partial<Record<DataHubColumnViewColumnType, Column>>;
    /**
     * Per-row item counts for countable attribute kinds (Tags, Glossary Terms). When a view sets
     * `display.custom.renderer` to a registry renderer (CHECK / COUNT) the kind's existing chip
     * `render` is swapped for the renderer fed with `countCell(count(record))`.
     */
    cellCounters?: Partial<Record<DataHubColumnViewColumnType, (record: ExtendedSchemaFields) => number>>;
    /** Renderer context shared by non-GRAPH cells (no loadMore; `limit` unused). */
    rendererContext?: Omit<ColumnRendererContext, 'column' | 'display'>;
    /**
     * Structured property columns. With no active view these are the platform-flagged ones
     * (useGetTableColumnProperties); with a view they are built from the view's own properties.
     * useGetStructuredPropColumns sets `key` to the property urn.
     */
    structuredPropColumns: Column[] | undefined;
    /** Read-only presence columns for LABEL kinds, keyed by column identity (see useLabelColumns). */
    labelColumns: Record<string, Column>;
    /** Renderers for RELATIONSHIP columns, keyed by column identity (see useRelationshipColumns). */
    relationshipColumns: Record<string, Column>;
}

/**
 * Turns the active definition (or its absence) into the ordered antd column list.
 * `undefined` definition = today's exact behavior (built-in default). Name is always first.
 */
/** Header drag-resize wiring. Absent = no grips (compact tables). */
export interface HeaderResizeHandlers {
    onResizeEnd: (columnId: string, width: number) => void;
    onReset: (columnId: string) => void;
    hint?: string;
}

/**
 * The built-in layout as ColumnLike entries in legacy order (Name excluded), so an ad hoc change
 * made with no view active — a resize, a toggle — has a definition to write into. Each entry
 * carries the legacy antd width as `display.width`, so once the synthesized definition is active
 * the untouched columns keep their hard-coded widths instead of snapping to the kind defaults.
 */
export function legacyDefaultColumns(sources: SchemaTableColumnSources): ColumnLike[] {
    const { byKind, structuredPropColumns } = sources;
    const legacyDisplay = (column: Column | undefined): Pick<ColumnLike, 'display'> =>
        typeof column?.width === 'number' ? { display: { width: column.width } } : {};
    let columns: ColumnLike[] = (
        [
            DataHubColumnViewColumnType.Type,
            DataHubColumnViewColumnType.Description,
            DataHubColumnViewColumnType.Tags,
            DataHubColumnViewColumnType.GlossaryTerms,
            DataHubColumnViewColumnType.Stats,
        ] as DataHubColumnViewColumnType[]
    )
        .filter((type) => byKind[type])
        .map((type) => ({ type, ...legacyDisplay(byKind[type]) }));
    if (byKind.BUSINESS_ATTRIBUTE) {
        columns = [
            ...columns,
            { type: DataHubColumnViewColumnType.BusinessAttribute, ...legacyDisplay(byKind.BUSINESS_ATTRIBUTE) },
        ];
    }
    if (structuredPropColumns) {
        const props: ColumnLike[] = structuredPropColumns
            .filter((c) => c.key)
            .map((c) => ({
                type: DataHubColumnViewColumnType.StructuredProperty,
                structuredPropertyParams: { urn: String(c.key) },
                ...legacyDisplay(c),
            }));
        columns.splice(columns.length - 1, 0, ...props);
    }
    return columns;
}

export function resolveSchemaTableColumns(
    sources: SchemaTableColumnSources,
    definition: Pick<DataHubColumnViewDefinition, 'columns'> | undefined,
    resize?: HeaderResizeHandlers,
): ColumnsType<ExtendedSchemaFields> {
    const { fieldColumn, byKind, structuredPropColumns, labelColumns, relationshipColumns } = sources;

    if (!definition) {
        // Legacy layout, verbatim from SchemaTable before Column Views (widths untouched).
        let columns: ColumnsType<ExtendedSchemaFields> = [
            fieldColumn,
            byKind.TYPE,
            byKind.DESCRIPTION,
            byKind.TAGS,
            byKind.GLOSSARY_TERMS,
            byKind.STATS,
        ].filter(Boolean) as ColumnsType<ExtendedSchemaFields>;
        if (byKind.BUSINESS_ATTRIBUTE) columns = [...columns, byKind.BUSINESS_ATTRIBUTE];
        if (structuredPropColumns) columns.splice(columns.length - 1, 0, ...structuredPropColumns);
        if (!resize) return columns;
        // legacyDefaultColumns mirrors this construction, so legacy[i - 1] is column i's ColumnLike.
        const legacy = legacyDefaultColumns(sources);
        return columns.map((column, i) => (i === 0 ? column : withResizeGrip(column, legacy[i - 1], resize)));
    }

    const resolved: ColumnsType<ExtendedSchemaFields> = [fieldColumn];
    definition.columns.forEach((col: ColumnLike) => {
        let column: Column | undefined;
        if (isStructuredPropertyColumn(col)) {
            const key = columnTableKey(col);
            column = structuredPropColumns?.find((c) => c.key === key);
        } else if (isLabelColumn(col)) {
            column = labelColumns[columnIdentity(col)];
        } else if (isRelationshipColumn(col)) {
            column = relationshipColumns[columnIdentity(col)];
        } else {
            column = byKind[col.type];
            if (column) column = withCountRenderer(column, col, sources);
        }
        if (column) resolved.push(withResizeGrip(applyDisplayWidth(column, col), col, resize));
    });
    return resolved;
}

/**
 * Routes a countable attribute cell (Tags / Glossary Terms) through the renderer registry when the
 * view picked a non-DEFAULT renderer. DEFAULT (or no counter / context) keeps the kind's existing
 * chip renderer untouched.
 */
function withCountRenderer(column: Column, col: ColumnLike, sources: SchemaTableColumnSources): Column {
    const count = sources.cellCounters?.[col.type];
    const key = customDisplayValue(col, RENDERER_CUSTOM_KEY);
    if (!count || !sources.rendererContext || !key || key === DEFAULT_RENDERER_KEY) return column;
    const renderer = resolveRenderer(col.type, key);
    if (!renderer || renderer.key === DEFAULT_RENDERER_KEY) return column;
    const ctx: ColumnRendererContext = { ...sources.rendererContext, column: col, display: displayFor(col) };
    return {
        ...column,
        render: (_: unknown, record: ExtendedSchemaFields) => renderer.render(countCell(count(record)), ctx),
    };
}

/**
 * `display.width ?? spec.defaultWidth`, never below `spec.minWidth`. Kinds with no default width
 * (Description) stay width-less so they grow. Header text truncates with a tooltip.
 */
function applyDisplayWidth(column: Column, col: ColumnLike): Column {
    const width = columnWidth(col);
    if (width === undefined) return column;
    return { ...column, width, ellipsis: { showTitle: true } };
}

/** Attaches the drag grip (ResizableHeaderCell) through antd's `onHeaderCell` pass-through props. */
function withResizeGrip(column: Column, col: ColumnLike | undefined, resize?: HeaderResizeHandlers): Column {
    if (!resize || !col) return column;
    const colviewResize: HeaderResizeProps = {
        columnId: columnIdentity(col),
        minWidth: COLUMN_KIND_SPECS[col.type].minWidth,
        onResizeEnd: resize.onResizeEnd,
        onReset: resize.onReset,
        hint: resize.hint,
    };
    type HeaderCellProps = ReturnType<NonNullable<Column['onHeaderCell']>>;
    return { ...column, onHeaderCell: () => ({ colviewResize } as unknown as HeaderCellProps) };
}

/** antd `sortOrder` for the view's default sort, applied to the matching column key. */
export function defaultSortForView(definition: Pick<DataHubColumnViewDefinition, 'sort'> | undefined) {
    const sort = definition?.sort;
    if (!sort) return undefined;
    return {
        columnKey: columnTableKey(sort.column),
        order: sort.order === 'ASCENDING' ? 'ascend' : 'descend',
    } as const;
}
