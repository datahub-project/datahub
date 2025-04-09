import { Tooltip } from '@components';
import { Typography } from 'antd';
import { Info } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import useExtractFieldGlossaryTermsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { EditableSchemaMetadata, EntityType, GlobalTags, SchemaField } from '@types';

const TagDisclaimer = styled(Typography.Text)`
    color: ${colors.gray[500]};
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
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const extractFieldGlossaryTermsInfo = useExtractFieldGlossaryTermsInfo(editableSchemaMetadata);
    const extractFieldTagsInfo = useExtractFieldTagsInfo(editableSchemaMetadata);

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField) => {
        const { editableTerms, uneditableTerms } = extractFieldGlossaryTermsInfo(record);
        const { editableTags, uneditableTags } = extractFieldTagsInfo(record, tags);

        return (
            <div data-testid={`schema-field-${record.fieldPath}-${options.showTags ? 'tags' : 'terms'}`}>
                {/* If can edit, show disclaimer for uneditable tags */}
                {canEdit && options.showTags && !uneditableTags?.tags?.length && (
                    <Tooltip title="Uneditable tags have either been provided by Ingestion or are managed via GraphQL API.">
                        <TagDisclaimer type="secondary">
                            <Info /> Some tags are not editable.
                        </TagDisclaimer>
                    </Tooltip>
                )}
                <TagTermGroup
                    uneditableTags={options.showTags ? uneditableTags : null}
                    editableTags={options.showTags ? editableTags : null}
                    uneditableGlossaryTerms={options.showTerms ? uneditableTerms : null}
                    editableGlossaryTerms={options.showTerms ? editableTerms : null}
                    canRemove={canEdit}
                    buttonProps={{ size: 'small' }}
                    canAddTag={canEdit && options.showTags}
                    canAddTerm={canEdit && options.showTerms}
                    entityUrn={urn}
                    entityType={EntityType.Dataset}
                    entitySubresource={record.fieldPath}
                    highlightText={filterText}
                    refetch={refresh}
                    showOneAndCount={showOneAndCount}
                    fontSize={12}
                />
            </div>
        );
    };
    return tagAndTermRender;
}
