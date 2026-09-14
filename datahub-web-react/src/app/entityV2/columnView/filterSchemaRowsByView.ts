import { LABEL_FILTER_FIELDS, STRUCTURED_PROPERTY_FILTER_PREFIX } from '@app/entityV2/columnView/columnKinds';
import { deriveTypeParts } from '@app/entityV2/columnView/deriveTypeParts';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

import { DataHubViewFilter, EditableSchemaMetadata, FacetFilter, LogicalOperator } from '@types';

/**
 * Small client-side evaluator for a Column View's row filter over already-fetched schema rows.
 * Everything the filter reads came through the schema query under the caller's context, so this
 * is a pure projection.
 *
 * Fields: fieldPath | nativeDataType | description | tags | glossaryTerms | structuredProperties.<urn>
 * Operators: EQUAL | IN | EXISTS | IS_NULL | CONTAIN | START_WITH | END_WITH |
 *            GREATER_THAN | GREATER_THAN_OR_EQUAL_TO | LESS_THAN | LESS_THAN_OR_EQUAL_TO
 * Ordered comparisons are numeric when both sides parse as numbers, else string (locale) compare;
 * they are not applied to urn-valued fields (tags / glossaryTerms), which evaluate to true.
 * Label presence is just `tags` / `glossaryTerms` EQUAL <urn>; those fields read every source the
 * Tags / Glossary Terms columns render (see fieldLabelUrns), including user-edited labels.
 * `negated` inverts a clause. Unknown fields/operators evaluate to true (never hide rows by mistake).
 */

/** One `editableSchemaFieldInfo` entry: the user-edited tags / terms / description for a fieldPath. */
export type EditableFieldInfo = NonNullable<EditableSchemaMetadata['editableSchemaFieldInfo']>[number];

/** fieldPath → editable entry, so per-row lookups in sorters, renderers and filters are O(1). */
export function editableFieldInfoByPath(
    editableSchemaMetadata?: EditableSchemaMetadata | null,
): Map<string, EditableFieldInfo> {
    return new Map((editableSchemaMetadata?.editableSchemaFieldInfo || []).map((f) => [f.fieldPath, f]));
}

/**
 * Tag / term urns the field carries, from the same sources the Tags / Glossary Terms columns read:
 * the ingested schemaMetadata field, the user-edited editableSchemaMetadata entry for the same
 * fieldPath, and the schemaField entity's own `tags` / `glossaryTerms` aspects (selected by
 * getDatasetSchema under `$includeFieldLabels`, which schemaQueryIncludesFor turns on for any
 * TAGS / GLOSSARY_TERMS / LABEL column or label filter).
 */
export function fieldLabelUrns(
    record: ExtendedSchemaFields,
    editable?: EditableFieldInfo | null,
): { tags: string[]; terms: string[] } {
    const fieldEntity: any = (record as any).schemaFieldEntity;
    const tags = [
        ...(record.globalTags?.tags || []).map((t) => t.tag?.urn),
        ...(editable?.globalTags?.tags || []).map((t) => t.tag?.urn),
        ...(fieldEntity?.tags?.tags || []).map((t: any) => t.tag?.urn),
    ].filter(Boolean) as string[];
    const terms = [
        ...(record.glossaryTerms?.terms || []).map((t) => t.term?.urn),
        ...(editable?.glossaryTerms?.terms || []).map((t) => t.term?.urn),
        ...(fieldEntity?.glossaryTerms?.terms || []).map((t: any) => t.term?.urn),
    ].filter(Boolean) as string[];
    return { tags, terms };
}

/** Numeric compare when both sides are numbers, else string compare. `<0` when a < b. */
function compareValues(a: string, b: string): number {
    const na = Number(a);
    const nb = Number(b);
    if (a.trim() !== '' && b.trim() !== '' && !Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
    return a.localeCompare(b);
}

const ORDERED: Record<string, (cmp: number) => boolean> = {
    GREATER_THAN: (cmp) => cmp > 0,
    GREATER_THAN_OR_EQUAL_TO: (cmp) => cmp >= 0,
    LESS_THAN: (cmp) => cmp < 0,
    LESS_THAN_OR_EQUAL_TO: (cmp) => cmp <= 0,
};

type Row = ExtendedSchemaFields & Record<string, any>;

/**
 * Values of `field` for a row. Urn-valued fields return urns; scalars return their string form.
 * `editable` is the row's own editableSchemaMetadata entry (see editableFieldInfoByPath).
 */
export function rowFieldValues(row: Row, field: string, editable?: EditableFieldInfo | null): string[] {
    switch (field) {
        case 'fieldPath':
            return row.fieldPath ? [row.fieldPath] : [];
        case 'nativeDataType':
            return row.nativeDataType ? [row.nativeDataType] : [];
        case 'nullable':
            return [String(Boolean(row.nullable))];
        case 'isPartOfKey':
            return [String(Boolean(row.isPartOfKey))];
        case 'isPartitioningKey':
            return [String(Boolean(row.isPartitioningKey))];
        case 'length':
        case 'precision':
        case 'scale': {
            const part = deriveTypeParts(row.nativeDataType, row.type)[field];
            return part === undefined ? [] : [String(part)];
        }
        case 'description':
            return row.description ? [row.description] : [];
        case 'tags':
            return fieldLabelUrns(row, editable).tags;
        case 'glossaryTerms':
            return fieldLabelUrns(row, editable).terms;
        default:
            if (field.startsWith(STRUCTURED_PROPERTY_FILTER_PREFIX)) {
                const urn = field.slice(STRUCTURED_PROPERTY_FILTER_PREFIX.length);
                const props = row.schemaFieldEntity?.structuredProperties?.properties || [];
                const match = props.find((p: any) => p.structuredProperty?.urn === urn);
                return (match?.values || [])
                    .map((v: any) => v?.stringValue ?? (v?.numberValue !== undefined ? String(v.numberValue) : undefined))
                    .filter((v: any) => v !== undefined && v !== null);
            }
            return [];
    }
}

const KNOWN_FIELDS = [
    'fieldPath',
    'nativeDataType',
    'nullable',
    'isPartOfKey',
    'isPartitioningKey',
    'length',
    'precision',
    'scale',
    'description',
    'tags',
    'glossaryTerms',
];

export function evaluateClause(
    row: Row,
    clause: Pick<FacetFilter, 'field' | 'condition' | 'values' | 'negated'>,
    editable?: EditableFieldInfo | null,
): boolean {
    const { field, values = [] } = clause;
    const condition = String(clause.condition ?? 'EQUAL');
    if (!KNOWN_FIELDS.includes(field) && !field.startsWith(STRUCTURED_PROPERTY_FILTER_PREFIX)) return true;
    const actual = rowFieldValues(row, field, editable);
    let result: boolean;
    switch (condition) {
        case 'EQUAL':
        case 'IN':
            result = actual.some((a) => values.includes(a));
            break;
        case 'CONTAIN':
            result = actual.some((a) => values.some((v) => a.toLowerCase().includes(v.toLowerCase())));
            break;
        case 'EXISTS':
            result = actual.length > 0;
            break;
        case 'IS_NULL':
            result = actual.length === 0;
            break;
        case 'START_WITH':
            result = actual.some((a) => values.some((v) => a.toLowerCase().startsWith(v.toLowerCase())));
            break;
        case 'END_WITH':
            result = actual.some((a) => values.some((v) => a.toLowerCase().endsWith(v.toLowerCase())));
            break;
        case 'GREATER_THAN':
        case 'GREATER_THAN_OR_EQUAL_TO':
        case 'LESS_THAN':
        case 'LESS_THAN_OR_EQUAL_TO': {
            // Ordering label urns is meaningless; leave such clauses neutral.
            if (LABEL_FILTER_FIELDS.includes(field)) {
                result = true;
                break;
            }
            const test = ORDERED[condition];
            result = actual.some((a) => values.some((v) => test(compareValues(a, v))));
            break;
        }
        default:
            result = true;
    }
    return clause.negated ? !result : result;
}

export function evaluateFilter(
    row: Row,
    filter: Pick<DataHubViewFilter, 'operator' | 'filters'> | null | undefined,
    editable?: EditableFieldInfo | null,
): boolean {
    if (!filter || !filter.filters?.length) return true;
    const results = filter.filters.map((c) => evaluateClause(row, c, editable));
    return filter.operator === LogicalOperator.Or ? results.some(Boolean) : results.every(Boolean);
}

/**
 * Filters top-level rows; a nested (struct) child row is kept when it or any descendant matches,
 * so the tree stays navigable. Returns the rows plus the pre/post counts for the
 * "Showing X of Y fields" line. `editableSchemaMetadata` lets tags / glossaryTerms clauses see
 * user-added labels, which are merged into the rows only at render time.
 */
export function filterSchemaRowsByView<T extends Row>(
    rows: T[],
    filter: Pick<DataHubViewFilter, 'operator' | 'filters'> | null | undefined,
    editableSchemaMetadata?: EditableSchemaMetadata | null,
): { rows: T[]; total: number; shown: number } {
    const total = countRows(rows);
    if (!filter || !filter.filters?.length) return { rows, total, shown: total };
    const editableByPath = editableFieldInfoByPath(editableSchemaMetadata);
    const keep = (row: T): T | undefined => {
        const children = ((row as any).children || []).map(keep).filter(Boolean) as T[];
        if (evaluateFilter(row, filter, editableByPath.get(row.fieldPath)) || children.length) {
            return children.length || (row as any).children ? ({ ...row, children: children.length ? children : undefined } as T) : row;
        }
        return undefined;
    };
    const filtered = rows.map(keep).filter(Boolean) as T[];
    return { rows: filtered, total, shown: countRows(filtered) };
}

function countRows(rows: Row[]): number {
    return rows.reduce((n, r) => n + 1 + countRows(((r as any).children || []) as Row[]), 0);
}
