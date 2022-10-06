import { ColumnEdge, FetchedEntity } from '../types';
import { SchemaField } from '../../../types.generated';

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

export function populateColumnsByUrn(
    columnsByUrn: Record<string, SchemaField[]>,
    fetchedEntities: { [x: string]: FetchedEntity },
    setColumnsByUrn: (colsByUrn: Record<string, SchemaField[]>) => void,
) {
    let populatedColumnsByUrn = { ...columnsByUrn };
    Object.entries(fetchedEntities).forEach(([urn, fetchedEntity]) => {
        if (fetchedEntity.schemaMetadata) {
            populatedColumnsByUrn = { ...populatedColumnsByUrn, [urn]: fetchedEntity.schemaMetadata.fields };
        }
    });
    setColumnsByUrn(populatedColumnsByUrn);
}

export function haveDisplayedFieldsChanged(displayedFields: SchemaField[], previousDisplayedFields?: SchemaField[]) {
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
