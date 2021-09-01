import {
    EditableSchemaFieldInfo,
    EditableSchemaFieldInfoUpdate,
    EditableSchemaMetadata,
    EntityType,
    GlobalTagsUpdate,
    Maybe,
    SchemaMetadata,
} from '../../../../../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../../../../../analytics';
import { convertTagsForUpdate } from '../../../../../../shared/tags/utils/convertTagsForUpdate';
import { ExtendedSchemaFields } from '../../../../../dataset/profile/schema/utils/types';
import {
    convertEditableSchemaMetadataForUpdate,
    pathMatchesNewPath,
} from '../../../../../dataset/profile/schema/utils/utils';
import { useEntityData, useEntityUpdate } from '../../../../EntityContext';

export const useUpdateSchema = (
    schema?: Maybe<SchemaMetadata>,
    editableSchemaMetadata?: Maybe<EditableSchemaMetadata>,
) => {
    const updateEntity = useEntityUpdate();
    const { urn } = useEntityData();

    const updateEditableSchema = (update) => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        return updateEntity({ variables: { input: { urn, editableSchemaMetadata: update } } });
    };

    const updateSchema = (newFieldInfo: EditableSchemaFieldInfoUpdate, record?: EditableSchemaFieldInfo) => {
        let existingMetadataAsUpdate = convertEditableSchemaMetadataForUpdate(editableSchemaMetadata);

        if (
            existingMetadataAsUpdate.editableSchemaFieldInfo.some((field) =>
                pathMatchesNewPath(field.fieldPath, record?.fieldPath),
            )
        ) {
            // if we already have a record for this field, update the record
            existingMetadataAsUpdate = {
                editableSchemaFieldInfo: existingMetadataAsUpdate.editableSchemaFieldInfo.map((fieldUpdate) => {
                    if (pathMatchesNewPath(fieldUpdate.fieldPath, record?.fieldPath)) {
                        return newFieldInfo;
                    }

                    // migrate any old fields that exist
                    const upgradedFieldPath = schema?.fields.find((field) =>
                        pathMatchesNewPath(fieldUpdate.fieldPath, field.fieldPath),
                    )?.fieldPath;

                    if (upgradedFieldPath) {
                        // eslint-disable-next-line no-param-reassign
                        fieldUpdate.fieldPath = upgradedFieldPath;
                    }

                    return fieldUpdate;
                }),
            };
        } else {
            // otherwise add a new record
            existingMetadataAsUpdate.editableSchemaFieldInfo.push(newFieldInfo);
        }
        return updateEditableSchema(existingMetadataAsUpdate);
    };

    const onUpdateTags = (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => {
        if (!record) return Promise.resolve();
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaTags,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: record?.description,
            globalTags: update,
        };
        return updateSchema(newFieldInfo, record);
    };

    const onUpdateDescription = (
        updatedDescription: string,
        record?: EditableSchemaFieldInfo | ExtendedSchemaFields,
    ) => {
        if (!record) return Promise.resolve();
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: updatedDescription,
            globalTags: { tags: convertTagsForUpdate(record?.globalTags?.tags || []) },
        };
        return updateSchema(newFieldInfo, record as EditableSchemaFieldInfo);
    };

    return { updateSchema, onUpdateTags, onUpdateDescription };
};
