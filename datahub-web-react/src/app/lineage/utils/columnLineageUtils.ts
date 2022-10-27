import { ColumnEdge, FetchedEntity, NodeData } from '../types';
import { InputFields, SchemaField } from '../../../types.generated';
import { downgradeV2FieldPath } from '../../entity/dataset/profile/schema/utils/utils';

export function getHighlightedColumnsForNode(highlightedEdges: ColumnEdge[], fields: SchemaField[], nodeUrn: string) {
    return highlightedEdges
        .filter(
            (edge) =>
                (edge.sourceUrn === nodeUrn && !!fields?.find((field) => field.fieldPath === edge.sourceField)) ||
                (edge.targetUrn === nodeUrn && !!fields?.find((field) => field.fieldPath === edge.targetField)),
        )
        .map((edge) => {
            if (edge.sourceUrn === nodeUrn) {
                return edge.sourceField;
            }
            if (edge.targetUrn === nodeUrn) {
                return edge.targetField;
            }
            return '';
        });
}

export function sortRelatedLineageColumns(
    highlightedColumnsForNode: string[],
    fields: SchemaField[],
    nodeUrn: string,
    columnsByUrn: Record<string, SchemaField[]>,
) {
    return {
        ...columnsByUrn,
        [nodeUrn || 'noop']: [...fields].sort(
            (fieldA, fieldB) =>
                highlightedColumnsForNode.indexOf(fieldB.fieldPath) -
                highlightedColumnsForNode.indexOf(fieldA.fieldPath),
        ),
    };
}

export function convertFieldsToV1FieldPath(fields: SchemaField[]) {
    return fields.map((field) => ({
        ...field,
        fieldPath: downgradeV2FieldPath(field.fieldPath) || '',
    }));
}

export function sortColumnsByDefault(
    columnsByUrn: Record<string, SchemaField[]>,
    fields: SchemaField[],
    nodeFields: SchemaField[],
    nodeUrn: string,
) {
    return {
        ...columnsByUrn,
        [nodeUrn || 'noop']: [...fields].sort(
            (fieldA, fieldB) =>
                (nodeFields.findIndex((field) => field.fieldPath === fieldA.fieldPath) || 0) -
                (nodeFields.findIndex((field) => field.fieldPath === fieldB.fieldPath) || 0),
        ),
    };
}

export function convertInputFieldsToSchemaFields(inputFields?: InputFields) {
    return inputFields?.fields?.map((field) => field?.schemaField) as SchemaField[] | undefined;
}

export function populateColumnsByUrn(
    columnsByUrn: Record<string, SchemaField[]>,
    fetchedEntities: { [x: string]: FetchedEntity },
    setColumnsByUrn: (colsByUrn: Record<string, SchemaField[]>) => void,
) {
    let populatedColumnsByUrn = { ...columnsByUrn };
    Object.entries(fetchedEntities).forEach(([urn, fetchedEntity]) => {
        if (fetchedEntity.schemaMetadata && !columnsByUrn[urn]) {
            populatedColumnsByUrn = {
                ...populatedColumnsByUrn,
                [urn]: convertFieldsToV1FieldPath(fetchedEntity.schemaMetadata.fields),
            };
        } else if (fetchedEntity.inputFields?.fields && !columnsByUrn[urn]) {
            populatedColumnsByUrn = {
                ...populatedColumnsByUrn,
                [urn]: convertFieldsToV1FieldPath(
                    convertInputFieldsToSchemaFields(fetchedEntity.inputFields) as SchemaField[],
                ),
            };
        }
    });
    setColumnsByUrn(populatedColumnsByUrn);
}

export function haveDisplayedFieldsChanged(displayedFields: SchemaField[], previousDisplayedFields?: SchemaField[]) {
    if (!previousDisplayedFields) return true;
    let hasChanged = false;
    displayedFields.forEach((field, index) => {
        if (
            previousDisplayedFields &&
            previousDisplayedFields[index] &&
            (previousDisplayedFields[index] as any).fieldPath !== field.fieldPath
        ) {
            hasChanged = true;
        }
    });
    return hasChanged;
}

export function filterColumns(
    filterText: string,
    node: { x: number; y: number; data: Omit<NodeData, 'children'> },
    setColumnsByUrn: (value: React.SetStateAction<Record<string, SchemaField[]>>) => void,
) {
    const filteredFields = node.data.schemaMetadata?.fields.filter((field) => field.fieldPath.includes(filterText));
    if (filteredFields) {
        setColumnsByUrn((colsByUrn) => ({
            ...colsByUrn,
            [node.data.urn || 'noop']: convertFieldsToV1FieldPath(filteredFields),
        }));
    }
}

export function getSourceUrnFromSchemaFieldUrn(schemaFieldUrn: string) {
    return schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[0].concat(')');
}
export function getFieldPathFromSchemaFieldUrn(schemaFieldUrn: string) {
    return schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[1].replace(',', '');
}
