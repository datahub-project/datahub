import { ColumnDisplayLike } from '@app/entityV2/columnView/types';

import {
    DataHubColumnViewColumn,
    DataHubColumnViewColumnInput,
    DataHubColumnViewColumnType,
    DataHubColumnViewExpand,
    DataHubColumnViewOverflow,
    DataHubViewFilter,
} from '@types';

/**
 * THE single frontend home for column-kind knowledge. Every other Column View file (builder,
 * select, SchemaTable resolution, fetch gating, cell rendering) goes through this module.
 *
 * Dispatch is always a single lookup on `type`. Relationship columns are LEAF kinds; their graph
 * edge lives in the spec table below (mirrors ColumnViewColumnKinds on the server), never in the
 * stored column. `display` is presentation only and never part of identity.
 */

export type ColumnKind = DataHubColumnViewColumnType;

/** Where a kind's values come from. GRAPH kinds are fetched separately, per visible row. */
export type ColumnSource = 'ASPECT' | 'FIELD_ENTITY' | 'GRAPH';

export interface ColumnKindSpec {
    kind: ColumnKind;
    /** i18n key for the header / picker label. */
    labelKey: string;
    /** Key used by the legacy hard-coded SchemaTable columns, when one exists. */
    legacyTableKey?: string;
    /** Builder group this kind is offered under. */
    group: 'attributes' | 'structuredProperties' | 'labels' | 'relationships';
    source: ColumnSource;
    /** GRAPH kinds only: the edge and whether we read it from the destination side. */
    edge?: { name: string; incoming: boolean };
    /** GRAPH kinds that can fan out to thousands of items; the UI caps, pages and hints. */
    highFanout?: boolean;
    /** At most one related item by construction; renders one chip, no count. */
    singleValued?: boolean;
    /** Only offered / labelled when logical models are enabled. */
    requiresLogicalModels?: boolean;
    /** Default antd width. `undefined` = width-less so the column grows (Description). */
    defaultWidth?: number;
    minWidth: number;
    /** Default `display` merged under the stored one. */
    defaultDisplay?: ColumnDisplayLike;
}

export const MAX_GRAPH_COLUMNS = 3;

const HIGH_FANOUT_DISPLAY: ColumnDisplayLike = {
    maxItems: 3,
    overflow: DataHubColumnViewOverflow.Count,
    expand: DataHubColumnViewExpand.Popover,
};

const attribute = (kind: ColumnKind, labelKey: string, legacyTableKey: string, defaultWidth?: number): ColumnKindSpec => ({
    kind,
    labelKey,
    legacyTableKey,
    group: 'attributes',
    source: 'ASPECT',
    defaultWidth,
    minWidth: 80,
});

const graph = (
    kind: ColumnKind,
    labelKey: string,
    edge: { name: string; incoming: boolean },
    opts: Pick<ColumnKindSpec, 'highFanout' | 'singleValued' | 'requiresLogicalModels'> = {},
): ColumnKindSpec => ({
    kind,
    labelKey,
    group: 'relationships',
    source: 'GRAPH',
    edge,
    defaultWidth: 220,
    minWidth: 120,
    defaultDisplay: opts.highFanout ? HIGH_FANOUT_DISPLAY : undefined,
    ...opts,
});

export const COLUMN_KIND_SPECS: Record<ColumnKind, ColumnKindSpec> = {
    [DataHubColumnViewColumnType.Type]: attribute(DataHubColumnViewColumnType.Type, 'common.labels:type', 'type', 100),
    // Field attributes that the legacy table never had as columns (native type is its Type tooltip;
    // nullable / key flags are pills on the name). Built by useFieldAttributeColumns.
    [DataHubColumnViewColumnType.NativeType]: attribute(
        DataHubColumnViewColumnType.NativeType,
        'columnViews.kind.nativeType',
        'nativeType',
        160,
    ),
    [DataHubColumnViewColumnType.Length]: attribute(DataHubColumnViewColumnType.Length, 'columnViews.kind.length', 'length', 90),
    [DataHubColumnViewColumnType.PrecisionScale]: attribute(
        DataHubColumnViewColumnType.PrecisionScale,
        'columnViews.kind.precisionScale',
        'precisionScale',
        130,
    ),
    [DataHubColumnViewColumnType.Nullable]: attribute(
        DataHubColumnViewColumnType.Nullable,
        'entity.profile.schema:constraintLabels.nullable',
        'nullable',
        90,
    ),
    [DataHubColumnViewColumnType.PrimaryKey]: attribute(
        DataHubColumnViewColumnType.PrimaryKey,
        'entity.profile.schema:constraintLabels.primaryKey',
        'primaryKey',
        110,
    ),
    [DataHubColumnViewColumnType.PartitionKey]: attribute(
        DataHubColumnViewColumnType.PartitionKey,
        'entity.profile.schema:constraintLabels.partitionKey',
        'partitionKey',
        120,
    ),
    [DataHubColumnViewColumnType.Description]: attribute(
        DataHubColumnViewColumnType.Description,
        'common.labels:description',
        'description',
    ),
    [DataHubColumnViewColumnType.Tags]: attribute(DataHubColumnViewColumnType.Tags, 'common.labels:tags', 'tag', 200),
    [DataHubColumnViewColumnType.GlossaryTerms]: attribute(
        DataHubColumnViewColumnType.GlossaryTerms,
        'entity.profile.schema:schemaTable.glossaryTermsColumn',
        'term',
        200,
    ),
    [DataHubColumnViewColumnType.BusinessAttribute]: attribute(
        DataHubColumnViewColumnType.BusinessAttribute,
        'entity.profile.schema:schemaTable.businessAttributeColumn',
        'businessAttribute',
        200,
    ),
    [DataHubColumnViewColumnType.Stats]: attribute(
        DataHubColumnViewColumnType.Stats,
        'entity.profile.schema:schemaTable.statsColumn',
        'usage',
        200,
    ),
    [DataHubColumnViewColumnType.LogicalParent]: graph(
        DataHubColumnViewColumnType.LogicalParent,
        'columnViews.relationship.logicalParent',
        { name: 'PhysicalInstanceOf', incoming: false },
        { singleValued: true, requiresLogicalModels: true },
    ),
    [DataHubColumnViewColumnType.PhysicalChildren]: graph(
        DataHubColumnViewColumnType.PhysicalChildren,
        'columnViews.relationship.physicalChildren',
        { name: 'PhysicalInstanceOf', incoming: true },
        { highFanout: true, requiresLogicalModels: true },
    ),
    [DataHubColumnViewColumnType.UpstreamColumns]: graph(
        DataHubColumnViewColumnType.UpstreamColumns,
        'columnViews.relationship.upstreamColumns',
        { name: 'DownstreamOf', incoming: false },
        { highFanout: true },
    ),
    [DataHubColumnViewColumnType.DownstreamColumns]: graph(
        DataHubColumnViewColumnType.DownstreamColumns,
        'columnViews.relationship.downstreamColumns',
        { name: 'DownstreamOf', incoming: true },
        { highFanout: true },
    ),
    [DataHubColumnViewColumnType.ForeignKeyTo]: graph(
        DataHubColumnViewColumnType.ForeignKeyTo,
        'columnViews.relationship.foreignKeyTo',
        { name: 'ForeignKeyTo', incoming: false },
    ),
    [DataHubColumnViewColumnType.ReferencedBy]: graph(
        DataHubColumnViewColumnType.ReferencedBy,
        'columnViews.relationship.referencedBy',
        { name: 'ForeignKeyTo', incoming: true },
        { highFanout: true },
    ),
    [DataHubColumnViewColumnType.StructuredProperty]: {
        kind: DataHubColumnViewColumnType.StructuredProperty,
        labelKey: 'columnViews.kind.structuredProperty',
        group: 'structuredProperties',
        source: 'FIELD_ENTITY',
        defaultWidth: 200,
        minWidth: 80,
    },
    [DataHubColumnViewColumnType.Label]: {
        kind: DataHubColumnViewColumnType.Label,
        labelKey: 'columnViews.kind.label',
        group: 'labels',
        source: 'FIELD_ENTITY',
        defaultWidth: 120,
        minWidth: 60,
    },
};

/** Kinds offered as single toggles in the "Field attributes" group. */
export const ATTRIBUTE_KINDS: ColumnKind[] = Object.values(COLUMN_KIND_SPECS)
    .filter((s) => s.group === 'attributes')
    .map((s) => s.kind);

/** Relationship leaves, in builder order. */
export const GRAPH_KINDS: ColumnKind[] = Object.values(COLUMN_KIND_SPECS)
    .filter((s) => s.source === 'GRAPH')
    .map((s) => s.kind);

export function availableRelationshipKinds(logicalModelsEnabled: boolean): ColumnKindSpec[] {
    return GRAPH_KINDS.map((k) => COLUMN_KIND_SPECS[k]).filter((s) => logicalModelsEnabled || !s.requiresLogicalModels);
}

/* ------------------------------------------------------------------------------------------ */
/* Column-like shapes: output (DataHubColumnViewColumn) and input (DataHubColumnViewColumnInput) */
/* ------------------------------------------------------------------------------------------ */

/** Resolved label entity (Tag | GlossaryTerm) as selected by the columnViewColumn fragment. */
export type LabelEntityLike = {
    urn: string;
    type?: string;
    name?: string | null;
    properties?: { name?: string | null } | null;
};

/** The slice of a hydrated StructuredPropertyEntity a column may carry (views + picker hydrate it). */
export type StructuredPropertyEntityLike = {
    urn: string;
    definition?: { displayName?: string | null; qualifiedName?: string | null } | null;
};

/** Superset of the output and input shapes so identity/header helpers accept either. */
export type ColumnLike = Pick<DataHubColumnViewColumn, 'type'> & {
    structuredPropertyParams?: { structuredProperty?: StructuredPropertyEntityLike | null; urn?: string | null } | null;
    labelParams?: { label?: LabelEntityLike | null; urn?: string | null } | null;
    display?: ColumnDisplayLike | null;
};

export const structuredPropertyUrnOf = (c: ColumnLike): string | undefined =>
    c.structuredPropertyParams?.structuredProperty?.urn ?? c.structuredPropertyParams?.urn ?? undefined;

/** Friendly name when the column carries a hydrated property; undefined for urn-only columns. */
export const structuredPropertyNameOf = (c: ColumnLike): string | undefined =>
    c.structuredPropertyParams?.structuredProperty?.definition?.displayName ||
    c.structuredPropertyParams?.structuredProperty?.definition?.qualifiedName ||
    undefined;

export const labelUrnOf = (c: ColumnLike): string | undefined =>
    c.labelParams?.label?.urn ?? c.labelParams?.urn ?? undefined;

export const labelNameOf = (c: ColumnLike): string | undefined =>
    c.labelParams?.label?.properties?.name ?? c.labelParams?.label?.name ?? undefined;

/**
 * Stable identity for de-dup, dnd-kit ids and table keys. Mirrors ColumnViewColumnKinds.identity:
 *   TYPE | UPSTREAM_COLUMNS | STRUCTURED_PROPERTY:<urn> | LABEL:<urn>   (display is NOT included)
 */
export function columnIdentity(column: ColumnLike): string {
    const sp = structuredPropertyUrnOf(column);
    if (sp) return `${column.type}:${sp}`;
    const label = labelUrnOf(column);
    if (label) return `${column.type}:${label}`;
    return column.type;
}

/**
 * The antd column `key` a resolved column should carry so sorting/visibility keep working.
 * Unparameterized kinds keep their legacy key (so the built-in default is byte-identical);
 * structured properties use the bare urn (as useGetStructuredPropColumns does); labels and
 * relationships use the identity.
 */
export function columnTableKey(column: ColumnLike): string {
    const spec = COLUMN_KIND_SPECS[column.type];
    if (spec.group === 'structuredProperties') return structuredPropertyUrnOf(column) ?? column.type;
    if (spec.group === 'labels' || spec.group === 'relationships') return columnIdentity(column);
    return spec.legacyTableKey ?? column.type;
}

/** Strip nulls / server-only fields so a stored `display` round-trips as input. */
export function toDisplayInput(display?: ColumnDisplayLike | null): ColumnDisplayLike | undefined {
    if (!display) return undefined;
    const out: ColumnDisplayLike = {};
    if (display.width != null) out.width = display.width;
    if (display.labelStyle != null) out.labelStyle = display.labelStyle;
    if (display.overflow != null) out.overflow = display.overflow;
    if (display.expand != null) out.expand = display.expand;
    if (display.maxItems != null) out.maxItems = display.maxItems;
    if (display.custom?.length) out.custom = display.custom.map(({ key, value }) => ({ key, value }));
    return Object.keys(out).length ? out : undefined;
}

export function toColumnInput(column: ColumnLike): DataHubColumnViewColumnInput {
    const input: DataHubColumnViewColumnInput = { type: column.type };
    const sp = structuredPropertyUrnOf(column);
    if (sp) input.structuredPropertyParams = { urn: sp };
    const label = labelUrnOf(column);
    if (label) input.labelParams = { urn: label };
    const display = toDisplayInput(column.display);
    if (display) input.display = display as any;
    return input;
}

/** The column's effective `display`: spec default under the stored one. */
export function displayFor(column: ColumnLike): ColumnDisplayLike {
    return { ...(COLUMN_KIND_SPECS[column.type].defaultDisplay || {}), ...(toDisplayInput(column.display) || {}) };
}

/** A `display.custom` entry by key (e.g. renderer). */
export function customDisplayValue(column: ColumnLike, key: string): string | undefined {
    return displayFor(column).custom?.find((e) => e.key === key)?.value ?? undefined;
}

/** Return a copy of `column` with `display` patched (identity unchanged). */
export function withDisplay<C extends ColumnLike>(column: C, patch: Partial<ColumnDisplayLike>): C {
    return { ...column, display: { ...(toDisplayInput(column.display) || {}), ...patch } };
}

const LABEL_URN_PREFIXES = ['urn:li:tag:', 'urn:li:glossaryTerm:'];
export const isLabelUrn = (urn: string) => LABEL_URN_PREFIXES.some((p) => urn.startsWith(p));

/** Client-side mirror of ColumnViewColumnKinds.paramProblems (boolean form). */
export function isValidColumn(column: ColumnLike): boolean {
    const spec = COLUMN_KIND_SPECS[column.type];
    const sp = structuredPropertyUrnOf(column);
    const label = labelUrnOf(column);
    if (Boolean(sp) !== (spec.group === 'structuredProperties')) return false;
    if (Boolean(label) !== (spec.group === 'labels')) return false;
    if (label && !isLabelUrn(label)) return false;
    const d = column.display;
    if (d?.maxItems != null && (d.maxItems < 1 || d.maxItems > 20)) return false;
    if (d?.width != null && (d.width < 40 || d.width > 2000)) return false;
    return true;
}

export const isGraphColumn = (c: ColumnLike) => COLUMN_KIND_SPECS[c.type].source === 'GRAPH';
/** Alias kept for existing call sites (SchemaTable, resolveSchemaTableColumns). */
export const isRelationshipColumn = isGraphColumn;
export const isStructuredPropertyColumn = (c: ColumnLike) =>
    COLUMN_KIND_SPECS[c.type].group === 'structuredProperties';
export const isLabelColumn = (c: ColumnLike) => COLUMN_KIND_SPECS[c.type].group === 'labels';

/** GRAPH kinds are paged previews, never row attributes: no sort, no filter. */
export const isSortableColumn = (c: ColumnLike) => !isGraphColumn(c);
export const isFilterableKind = (kind: ColumnKind) => COLUMN_KIND_SPECS[kind].source !== 'GRAPH';

/**
 * Header text for a column. `t` is the i18n function. LABEL headers are the label's name.
 */
export function columnHeader(
    column: ColumnLike,
    t: (key: string) => string,
    opts: { logicalModelsEnabled?: boolean; structuredPropertyName?: string | null } = {},
): string {
    if (isStructuredPropertyColumn(column)) {
        return (
            opts.structuredPropertyName ||
            structuredPropertyNameOf(column) ||
            structuredPropertyUrnOf(column) ||
            t(COLUMN_KIND_SPECS[column.type].labelKey)
        );
    }
    if (isLabelColumn(column)) {
        return labelNameOf(column) || labelUrnOf(column) || t(COLUMN_KIND_SPECS[column.type].labelKey);
    }
    return t(COLUMN_KIND_SPECS[column.type].labelKey);
}

/**
 * Secondary detail for hover (the urn) on parameterized columns whose header is a friendly name;
 * undefined for built-in kinds and when the header already IS the urn.
 */
export function columnHeaderHint(column: ColumnLike, header: string): string | undefined {
    let urn: string | undefined;
    if (isStructuredPropertyColumn(column)) urn = structuredPropertyUrnOf(column);
    else if (isLabelColumn(column)) urn = labelUrnOf(column);
    return urn && urn !== header ? urn : undefined;
}

/** Effective width for the antd column: stored, else spec default; never below spec min. */
export function columnWidth(column: ColumnLike): number | undefined {
    const spec = COLUMN_KIND_SPECS[column.type];
    const width = displayFor(column).width ?? spec.defaultWidth;
    return width == null ? undefined : Math.max(spec.minWidth, width);
}

/* ------------------------------------------------------------------------------------------ */
/* Fetch gating: which optional selections the main schema query needs for a definition.       */
/* ------------------------------------------------------------------------------------------ */

export interface SchemaQueryIncludes {
    includeStructuredProperties: boolean;
    /** schemaFieldEntity { tags glossaryTerms } — the entity-level labels of each field. */
    includeFieldLabels: boolean;
    /** schemaFieldEntity { businessAttributes } — only when a Business Attribute column is shown. */
    includeBusinessAttribute: boolean;
}

/** Filter-field prefix the client-side evaluator understands (see filterSchemaRowsByView). */
export const STRUCTURED_PROPERTY_FILTER_PREFIX = 'structuredProperties.';
/** Filter fields whose values are label urns (see filterSchemaRowsByView). */
export const LABEL_FILTER_FIELDS = ['tags', 'glossaryTerms'];

/** Kinds that read a field's tags / glossary terms (the built-in chip columns and LABEL). */
const readsFieldLabels = (c: ColumnLike) =>
    isLabelColumn(c) ||
    c.type === DataHubColumnViewColumnType.Tags ||
    c.type === DataHubColumnViewColumnType.GlossaryTerms;

/**
 * Derive the `@include` booleans for getDatasetSchema from the active definition:
 * columns ∪ sort column ∪ filter fields. No active view = legacy behavior (everything is fetched,
 * so the default stays byte-identical). GRAPH columns are fetched by a separate query and never
 * touch this one.
 */
export function schemaQueryIncludesFor(definition?: {
    columns: ColumnLike[];
    sort?: { column: ColumnLike } | null;
    filter?: Pick<DataHubViewFilter, 'filters'> | null;
}): SchemaQueryIncludes {
    if (!definition) {
        return { includeStructuredProperties: true, includeFieldLabels: true, includeBusinessAttribute: true };
    }
    const cols: ColumnLike[] = [...definition.columns, ...(definition.sort ? [definition.sort.column] : [])];
    const filterFields = (definition.filter?.filters || []).map((f) => f.field);
    return {
        includeStructuredProperties:
            cols.some(isStructuredPropertyColumn) ||
            filterFields.some((f) => f.startsWith(STRUCTURED_PROPERTY_FILTER_PREFIX)),
        includeFieldLabels: cols.some(readsFieldLabels) || filterFields.some((f) => LABEL_FILTER_FIELDS.includes(f)),
        includeBusinessAttribute: cols.some((c) => c.type === DataHubColumnViewColumnType.BusinessAttribute),
    };
}

/**
 * The built-in default, i.e. today's SchemaTable behavior (Name is implicit). Business Attribute
 * and platform-flagged structured properties are appended at resolve time by feature flag, so the
 * built-in default cannot be expressed purely as a column list; `undefined` active view means
 * "legacy behavior" in resolveSchemaTableColumns.
 */
export const BUILT_IN_DEFAULT_COLUMNS: DataHubColumnViewColumnInput[] = [
    { type: DataHubColumnViewColumnType.Type },
    { type: DataHubColumnViewColumnType.Description },
    { type: DataHubColumnViewColumnType.Tags },
    { type: DataHubColumnViewColumnType.GlossaryTerms },
    { type: DataHubColumnViewColumnType.Stats },
];

/** Attribute columns not shown by default but always offered for toggling (no parameters needed). */
export const OPTIONAL_ATTRIBUTE_COLUMNS: DataHubColumnViewColumnInput[] = [
    { type: DataHubColumnViewColumnType.BusinessAttribute },
    { type: DataHubColumnViewColumnType.NativeType },
    { type: DataHubColumnViewColumnType.Length },
    { type: DataHubColumnViewColumnType.PrecisionScale },
    { type: DataHubColumnViewColumnType.Nullable },
    { type: DataHubColumnViewColumnType.PrimaryKey },
    { type: DataHubColumnViewColumnType.PartitionKey },
];
