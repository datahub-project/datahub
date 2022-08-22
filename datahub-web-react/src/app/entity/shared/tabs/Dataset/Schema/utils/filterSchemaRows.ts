import { EntityType } from '../../../../../../../types.generated';
import { ExtendedSchemaFields } from '../../../../../dataset/profile/schema/utils/types';
import EntityRegistry from '../../../../../EntityRegistry';

function shouldIncludeRow(row: ExtendedSchemaFields, filterText: string, filteredFieldPathsByMetadata: any) {
    return (
        row.fieldPath.toLocaleLowerCase().includes(filterText) ||
        row.description?.toLocaleLowerCase().includes(filterText) ||
        filteredFieldPathsByMetadata.includes(row.fieldPath) ||
        row.children?.find((child) => shouldIncludeRow(child, filterText, filteredFieldPathsByMetadata))
    );
}

export function filterSchemaRows(
    rows: ExtendedSchemaFields[],
    editableSchemaMetadata: any,
    filterText: string,
    entityRegistry: EntityRegistry,
) {
    if (!filterText) return rows;

    const rowsCopy = [...rows];

    const filteredFieldPathsByMetadata = editableSchemaMetadata?.editableSchemaFieldInfo
        .filter((fieldInfo) => {
            return (
                fieldInfo.description?.toLocaleLowerCase().includes(filterText) ||
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
        .map((fieldInfo) => fieldInfo.fieldPath);

    const filteredRows = rowsCopy.filter((row) => shouldIncludeRow(row, filterText, filteredFieldPathsByMetadata));

    const test = filteredRows.map((row) => {
        if (row.children) {
            row.children.splice(
                0,
                row.children.length,
                ...row.children.filter((child) => shouldIncludeRow(child, filterText, filteredFieldPathsByMetadata)),
            );
            // row.children.filter((child) => shouldIncludeRow(child, filterText, filteredFieldPathsByMetadata));
        }
        return row;
    });

    return test;
}
