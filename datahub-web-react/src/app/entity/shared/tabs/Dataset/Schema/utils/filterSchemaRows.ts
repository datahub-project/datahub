import { EntityType, SchemaField } from '../../../../../../../types.generated';
import EntityRegistry from '../../../../../EntityRegistry';

// returns list of fieldPaths for fields that have Terms or Tags matching the filterText
function getFilteredFieldPathsByMetadata(editableSchemaMetadata: any, entityRegistry, filterText) {
    return (
        editableSchemaMetadata?.editableSchemaFieldInfo
            .filter((fieldInfo) => {
                return (
                    fieldInfo.globalTags?.tags.find((tagAssociation) =>
                        entityRegistry
                            .getDisplayName(EntityType.Tag, tagAssociation.tag)
                            .toLocaleLowerCase()
                            .includes(filterText),
                    ) ||
                    fieldInfo.glossaryTerms?.terms.find((termAssociation) =>
                        entityRegistry
                            .getDisplayName(EntityType.GlossaryTerm, termAssociation.term)
                            .toLocaleLowerCase()
                            .includes(filterText),
                    )
                );
            })
            .map((fieldInfo) => fieldInfo.fieldPath) || []
    );
}

function shouldInclude(
    fieldName: string,
    fullFieldPath: string,
    filterText: string,
    filteredFieldPathsByMetadata: any,
) {
    return fieldName.toLocaleLowerCase().includes(filterText) || filteredFieldPathsByMetadata.includes(fullFieldPath);
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

    const filteredFieldPathsByMetadata = getFilteredFieldPathsByMetadata(
        editableSchemaMetadata,
        entityRegistry,
        formattedFilterText,
    );
    const finalFieldPaths = new Set();
    const expandedRowsFromFilter = new Set();

    rows.forEach((row) => {
        if (shouldInclude(row.fieldPath, row.fieldPath, formattedFilterText, filteredFieldPathsByMetadata)) {
            finalFieldPaths.add(row.fieldPath);
        }
        const splitFieldPath = row.fieldPath.split('.');
        const fieldName = splitFieldPath.slice(-1)[0];
        if (shouldInclude(fieldName, row.fieldPath, formattedFilterText, filteredFieldPathsByMetadata)) {
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
