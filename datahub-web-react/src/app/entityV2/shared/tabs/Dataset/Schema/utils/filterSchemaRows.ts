import { EntityRegistry } from '../../../../../../../entityRegistryContext';
import { EntityType, SchemaField } from '../../../../../../../types.generated';

export enum SchemaFilterType {
    FieldPath = 'Field Path',
    Documentation = 'Documentation',
    Tags = 'Tags',
    Terms = 'Terms',
}

function matchesTagsOrTermsOrDescription(
    field: SchemaField,
    filterText: string,
    entityRegistry: EntityRegistry,
    schemaFilterTypes: SchemaFilterType[],
) {
    return (
        (field.globalTags?.tags?.find((tagAssociation) =>
            entityRegistry.getDisplayName(EntityType.Tag, tagAssociation.tag).toLocaleLowerCase().includes(filterText),
        ) &&
            schemaFilterTypes.includes(SchemaFilterType.Tags)) ||
        (field.glossaryTerms?.terms?.find((termAssociation) =>
            entityRegistry
                .getDisplayName(EntityType.GlossaryTerm, termAssociation.term)
                .toLocaleLowerCase()
                .includes(filterText),
        ) &&
            schemaFilterTypes.includes(SchemaFilterType.Terms)) ||
        (field.description?.toLocaleLowerCase().includes(filterText) &&
            schemaFilterTypes.includes(SchemaFilterType.Documentation))
    );
}

// returns list of fieldPaths for fields that have Terms or Tags or Descriptions matching the filterText
function getFilteredFieldPathsByMetadata(
    editableSchemaMetadata: any,
    entityRegistry,
    filterText,
    schemaFilterTypes: SchemaFilterType[],
) {
    return (
        editableSchemaMetadata?.editableSchemaFieldInfo
            .filter((fieldInfo) =>
                matchesTagsOrTermsOrDescription(fieldInfo, filterText, entityRegistry, schemaFilterTypes),
            )
            .map((fieldInfo) => fieldInfo.fieldPath) || []
    );
}

function matchesEditableTagsOrTermsOrDescription(field: SchemaField, filteredFieldPathsByEditableMetadata: any) {
    return filteredFieldPathsByEditableMetadata.includes(field.fieldPath);
}

function matchesFieldName(fieldName: string, filterText: string, schemaFilterTypes: SchemaFilterType[]) {
    return fieldName.toLocaleLowerCase().includes(filterText) && schemaFilterTypes.includes(SchemaFilterType.FieldPath);
}

function returnNoFilterAndExpandedPathMatch(rows: SchemaField[], expandedDrawerFieldPath: string | null) {
    const expandedRowsFromFilter = new Set<string>();
    rows.forEach((row) => {
        const splitFieldPath = row.fieldPath.split('.');
        if (row.fieldPath === expandedDrawerFieldPath) {
            // if we match specifically on this field (not just its parent), add and expand all parents
            splitFieldPath.reduce((previous, current) => {
                expandedRowsFromFilter.add(previous);
                return `${previous}.${current}`;
            });
        }
    });
    return { filteredRows: rows, expandedRowsFromFilter, matches: [] };
}

export function filterSchemaRows(
    rows: SchemaField[],
    editableSchemaMetadata: any,
    filterText: string,
    schemaFilterTypes: SchemaFilterType[],
    // if they have the drawer expanded on some asset we don't want to let this collapse
    expandedDrawerFieldPath: string | null,
    entityRegistry: EntityRegistry,
    skipParents?: boolean,
) {
    if (!rows) return { filteredRows: [], expandedRowsFromFilter: new Set<string>() };

    if (!filterText && expandedDrawerFieldPath) {
        return returnNoFilterAndExpandedPathMatch(rows, expandedDrawerFieldPath);
    }
    if (!filterText || schemaFilterTypes.length === 0)
        return {
            filteredRows: rows,
            expandedRowsFromFilter: new Set<string>(),
        };

    const formattedFilterText = filterText.toLocaleLowerCase();

    const filteredFieldPathsByEditableMetadata = getFilteredFieldPathsByMetadata(
        editableSchemaMetadata,
        entityRegistry,
        formattedFilterText,
        schemaFilterTypes,
    );

    const finalFieldPaths = new Set();
    const expandedRowsFromFilter = new Set<string>();
    const matches: { path: string; index: number }[] = [];

    rows.forEach((row, idx) => {
        if (
            matchesFieldName(row.fieldPath, formattedFilterText, schemaFilterTypes) ||
            matchesEditableTagsOrTermsOrDescription(row, filteredFieldPathsByEditableMetadata) ||
            matchesTagsOrTermsOrDescription(row, formattedFilterText, entityRegistry, schemaFilterTypes) // non-editable tags, terms and description
        ) {
            finalFieldPaths.add(row.fieldPath);
        }
        const splitFieldPath = row.fieldPath.split('.');
        const fieldName = splitFieldPath.slice(-1)[0];
        if (
            !skipParents &&
            (matchesFieldName(fieldName, formattedFilterText, schemaFilterTypes) ||
                matchesEditableTagsOrTermsOrDescription(row, filteredFieldPathsByEditableMetadata) ||
                matchesTagsOrTermsOrDescription(row, formattedFilterText, entityRegistry, schemaFilterTypes)) // non-editable tags, terms and description
        ) {
            // if we match specifically on this field (not just its parent), add and expand all parents
            matches.push({ path: row.fieldPath, index: idx });
            splitFieldPath.reduce((previous, current) => {
                finalFieldPaths.add(previous);
                expandedRowsFromFilter.add(previous);
                return `${previous}.${current}`;
            });
        }
    });

    const filteredRows = rows.filter((row) => finalFieldPaths.has(row.fieldPath));

    return { filteredRows, expandedRowsFromFilter, matches };
}
