import { EntityType, SchemaField } from '../../../../../../../types.generated';
import EntityRegistry from '../../../../../EntityRegistry';

function matchesTagsOrTermsOrDescription(field: SchemaField, filterText: string, entityRegistry: EntityRegistry) {
    return (
        field.globalTags?.tags?.find((tagAssociation) =>
            entityRegistry.getDisplayName(EntityType.Tag, tagAssociation.tag).toLocaleLowerCase().includes(filterText),
        ) ||
        field.glossaryTerms?.terms?.find((termAssociation) =>
            entityRegistry
                .getDisplayName(EntityType.GlossaryTerm, termAssociation.term)
                .toLocaleLowerCase()
                .includes(filterText),
        ) ||
        field.description?.toLocaleLowerCase()?.includes(filterText)
    );
}

function matchesBusinessAttributesProperties(field: SchemaField, filterText: string, entityRegistry: EntityRegistry) {
    if (!field.schemaFieldEntity?.businessAttributes) return false;
    const businessAttributeProperties =
        field.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties;
    return (
        businessAttributeProperties?.description?.toLocaleLowerCase().includes(filterText) ||
        businessAttributeProperties?.name?.toLocaleLowerCase().includes(filterText) ||
        businessAttributeProperties?.glossaryTerms?.terms?.find((termAssociation) =>
            entityRegistry
                .getDisplayName(EntityType.GlossaryTerm, termAssociation.term)
                .toLocaleLowerCase()
                .includes(filterText),
        ) ||
        businessAttributeProperties?.tags?.tags?.find((tagAssociation) =>
            entityRegistry.getDisplayName(EntityType.Tag, tagAssociation.tag).toLocaleLowerCase().includes(filterText),
        )
    );
}

// returns list of fieldPaths for fields that have Terms or Tags or Descriptions matching the filterText
function getFilteredFieldPathsByMetadata(editableSchemaMetadata: any, entityRegistry, filterText) {
    return (
        editableSchemaMetadata?.editableSchemaFieldInfo
            .filter((fieldInfo) => matchesTagsOrTermsOrDescription(fieldInfo, filterText, entityRegistry))
            .map((fieldInfo) => fieldInfo.fieldPath) || []
    );
}

function matchesEditableTagsOrTermsOrDescription(field: SchemaField, filteredFieldPathsByEditableMetadata: any) {
    return filteredFieldPathsByEditableMetadata.includes(field.fieldPath);
}

function matchesFieldName(fieldName: string, filterText: string) {
    return fieldName.toLocaleLowerCase().includes(filterText);
}

export function filterSchemaRows(
    rows: SchemaField[],
    editableSchemaMetadata: any,
    filterText: string,
    entityRegistry: EntityRegistry,
) {
    if (!rows) return { filteredRows: [], expandedRowsFromFilter: new Set() };
    if (!filterText) return { filteredRows: rows, expandedRowsFromFilter: new Set() };
    const formattedFilterText = filterText.toLocaleLowerCase();

    const filteredFieldPathsByEditableMetadata = getFilteredFieldPathsByMetadata(
        editableSchemaMetadata,
        entityRegistry,
        formattedFilterText,
    );

    const finalFieldPaths = new Set();
    const expandedRowsFromFilter = new Set();

    rows.forEach((row) => {
        if (
            matchesFieldName(row.fieldPath, formattedFilterText) ||
            matchesEditableTagsOrTermsOrDescription(row, filteredFieldPathsByEditableMetadata) ||
            matchesTagsOrTermsOrDescription(row, formattedFilterText, entityRegistry) || // non-editable tags, terms and description
            matchesBusinessAttributesProperties(row, formattedFilterText, entityRegistry)
        ) {
            finalFieldPaths.add(row.fieldPath);
        }
        const splitFieldPath = row.fieldPath.split('.');
        const fieldName = splitFieldPath.slice(-1)[0];
        if (
            matchesFieldName(fieldName, formattedFilterText) ||
            matchesEditableTagsOrTermsOrDescription(row, filteredFieldPathsByEditableMetadata) ||
            matchesTagsOrTermsOrDescription(row, formattedFilterText, entityRegistry) || // non-editable tags, terms and description
            matchesBusinessAttributesProperties(row, formattedFilterText, entityRegistry)
        ) {
            // if we match specifically on this field (not just its parent), add and expand all parents
            splitFieldPath.reduce((previous, current) => {
                finalFieldPaths.add(previous);
                expandedRowsFromFilter.add(previous);
                return `${previous}.${current}`;
            });
        }
    });

    const filteredRows = rows.filter((row) => finalFieldPaths.has(row.fieldPath));

    return { filteredRows, expandedRowsFromFilter };
}
