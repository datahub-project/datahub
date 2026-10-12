import { FetchedEntity } from '@app/lineage/types';
import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';

import { EntityType, InputFields, SchemaField, SchemaFieldDataType } from '@types';

export function convertFieldsToV1FieldPath(fields: SchemaField[]) {
    return fields.map((field) => ({
        ...field,
        fieldPath: downgradeV2FieldPath(field.fieldPath) || '',
    }));
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

export function decodeSchemaField(fieldPath: string) {
    return fieldPath.replaceAll('%28', '(').replaceAll('%29', ')').replaceAll('%2C', ',');
}

export function encodeSchemaField(fieldPath: string) {
    return fieldPath.replaceAll('(', '%28').replaceAll(')', '%29').replaceAll(',', '%2C');
}
