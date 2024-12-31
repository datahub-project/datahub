import { ColumnEdge, FetchedEntity, NodeData } from '../types';
import { EntityType, InputFields, SchemaField, SchemaFieldDataType } from '../../../types.generated';
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

/*
 * Populate a columnsByUrn map with a list of columns per entity in the order that they will appear.
 * We need columnsByUrn in order to ensure that an entity does have a column that lineage data is
 * pointing to and to know where to draw column arrows in and out of the entity. DataJobs won't show columns
 * underneath them, but we need this populated for validating that this column "exists" on the entity.
 */
export function getPopulatedColumnsByUrn(
    columnsByUrn: Record<string, SchemaField[]>,
    fetchedEntities: Map<string, FetchedEntity>,
) {
    let populatedColumnsByUrn = { ...columnsByUrn };
    Array.from(fetchedEntities.entries()).forEach(([urn, fetchedEntity]) => {
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
        } else if (fetchedEntity.type === EntityType.DataJob && fetchedEntity.fineGrainedLineages) {
            // Add upstream and downstream fields from fineGrainedLineage onto DataJob to mimic upstream
            // and downstream dataset fields. DataJobs will virtually "have" these fields so we can draw
            // full column paths from upstream dataset fields to downstream dataset fields.
            const fields: SchemaField[] = [];
            fetchedEntity.fineGrainedLineages.forEach((fineGrainedLineage) => {
                fineGrainedLineage.upstreams?.forEach((upstream) => {
                    if (!fields.some((field) => field.fieldPath === upstream.path)) {
                        fields.push({
                            fieldPath: downgradeV2FieldPath(upstream.path) || '',
                            nullable: false,
                            recursive: false,
                            type: SchemaFieldDataType.String,
                        });
                    }
                });
                fineGrainedLineage.downstreams?.forEach((downstream) => {
                    if (!fields.some((field) => field.fieldPath === downstream.path)) {
                        fields.push({
                            fieldPath: downgradeV2FieldPath(downstream.path) || '',
                            nullable: false,
                            recursive: false,
                            type: SchemaFieldDataType.String,
                        });
                    }
                });
            });
            populatedColumnsByUrn = { ...populatedColumnsByUrn, [urn]: fields };
        }
    });
    return populatedColumnsByUrn;
}

export function populateColumnsByUrn(
    columnsByUrn: Record<string, SchemaField[]>,
    fetchedEntities: Map<string, FetchedEntity>,
    setColumnsByUrn: (colsByUrn: Record<string, SchemaField[]>) => void,
) {
    setColumnsByUrn(getPopulatedColumnsByUrn(columnsByUrn, fetchedEntities));
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
    const formattedFilterText = filterText.toLocaleLowerCase();
    const filteredFields = node.data.schemaMetadata?.fields?.filter((field) =>
        field.fieldPath.toLocaleLowerCase().includes(formattedFilterText),
    );
    if (filteredFields) {
        setColumnsByUrn((colsByUrn) => ({
            ...colsByUrn,
            [node.data.urn || 'noop']: convertFieldsToV1FieldPath(filteredFields),
        }));
    }
}

export function decodeSchemaField(fieldPath: string) {
    return fieldPath.replaceAll('%28', '(').replaceAll('%29', ')').replaceAll('%2C', ',');
}

export function encodeSchemaField(fieldPath: string) {
    return fieldPath.replaceAll('(', '%28').replaceAll(')', '%29').replaceAll(',', '%2C');
}

export function getSourceUrnFromSchemaFieldUrn(schemaFieldUrn: string) {
    return schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[0].concat(')');
}

export function getFieldPathFromSchemaFieldUrn(schemaFieldUrn: string) {
    return decodeSchemaField(schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[1].replace(',', ''));
}

export function isSameColumn({
    sourceUrn,
    targetUrn,
    sourceField,
    targetField,
}: {
    sourceUrn: string;
    targetUrn: string;
    sourceField: string;
    targetField: string;
}) {
    return sourceUrn === targetUrn && sourceField === targetField;
}
