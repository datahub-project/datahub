import { Text, Tooltip } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entityV2/shared/EntityContext';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import useEditableSchemaFieldInfoMaps, {
    EditableFieldInfoMaps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';
import useExtractFieldGlossaryTermsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '@types';

const TagDisclaimer = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    align-items: center;
    display: flex;
    gap: 4px;
    width: fit-content;
    margin-bottom: 4px;
`;

export default function useTagsAndTermsRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    options: { showTags: boolean; showTerms: boolean },
    filterText: string,
    canEdit: boolean,
    showOneAndCount?: boolean,
    fieldInfoMaps?: EditableFieldInfoMaps,
) {
    const { t } = useTranslation('entity.profile.schema');
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const entityRegistry = useEntityRegistry();
    const schemaRefetch = useSchemaRefetch();
    const fallbackMaps = useEditableSchemaFieldInfoMaps(fieldInfoMaps ? undefined : editableSchemaMetadata);
    const maps = fieldInfoMaps ?? fallbackMaps;
    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata, maps);
    const extractFieldTagsInfo = useExtractFieldTagsInfo(editableSchemaMetadata, maps);
    const { entityData } = useEntityData();
    const platformName = entityData?.platform
        ? entityRegistry.getDisplayName(EntityType.DataPlatform, entityData?.platform)
        : null;

    const { showTags, showTerms } = options;

    return useCallback(
        (tags: GlobalTags | null | undefined, record: SchemaField) => {
            const { directTerms, editableTerms, uneditableTerms } = extractFieldGlossaryTermsInfo(record);
            const { directTags, editableTags, uneditableTags } = extractFieldTagsInfo(record, tags);

            return (
                <div data-testid={`schema-field-${record.fieldPath}-${showTags ? 'tags' : 'terms'}`}>
                    {/* If can edit, show disclaimer for uneditable tags */}
                    {canEdit && showTags && !!uneditableTags?.tags?.length && (
                        <Tooltip
                            title={t('tagTermRenderer.externalPlatformTooltip', {
                                platform: platformName || t('tagTermRenderer.externalPlatformFallback'),
                            })}
                        >
                            <TagDisclaimer type="span">
                                <Info /> {t('tagTermRenderer.tagsNotEditable')}
                            </TagDisclaimer>
                        </Tooltip>
                    )}
                    <TagTermGroup
                        directTags={showTags ? directTags : null}
                        uneditableTags={showTags ? uneditableTags : null}
                        editableTags={showTags ? editableTags : null}
                        directGlossaryTerms={showTerms ? directTerms : null}
                        uneditableGlossaryTerms={showTerms ? uneditableTerms : null}
                        editableGlossaryTerms={showTerms ? editableTerms : null}
                        canRemove={canEdit}
                        buttonProps={{ size: 'small' }}
                        canAddTag={canEdit && showTags}
                        canAddTerm={canEdit && showTerms}
                        entityUrn={urn}
                        entityType={EntityType.Dataset}
                        entitySubresource={record.fieldPath}
                        highlightText={filterText}
                        refetch={async () => {
                            refetch?.();
                            schemaRefetch?.();
                        }}
                        showOneAndCount={showOneAndCount}
                        fontSize={12}
                    />
                </div>
            );
        },
        [
            extractFieldGlossaryTermsInfo,
            extractFieldTagsInfo,
            showTags,
            showTerms,
            canEdit,
            platformName,
            t,
            urn,
            filterText,
            showOneAndCount,
            refetch,
            schemaRefetch,
        ],
    );
}
