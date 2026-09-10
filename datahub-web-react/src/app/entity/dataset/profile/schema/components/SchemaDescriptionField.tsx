import { EditOutlined } from '@ant-design/icons';
import { FetchResult } from '@apollo/client';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Typography, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import StripMarkdownText, { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';
import PropagationDetails from '@app/entity/shared/propagation/PropagationDetails';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import { Button } from '@src/alchemy-components';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';
import CompactMarkdownViewer from '@src/app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';

import { UpdateDatasetMutation } from '@graphql/dataset.generated';
import { StringMapEntry } from '@types';

// Legacy V1 decorative colors. The diff-highlight greens/reds are visually distinct from the
// semantic theme tokens (bgSurfaceSuccess/Error would render a much paler / different shade),
// so they are preserved verbatim to keep the original appearance of this deprecated component.
/* eslint-disable rulesdir/no-hardcoded-colors -- (legacy-color) preserve original V1 appearance; semantic tokens differ visually */
const DIFF_ADDED_BG = '#b7eb8f99';
const DIFF_ADDED_BG_HOVER = '#b7eb8faa';
const DIFF_REMOVED_BG = '#ffa39e99';
const DIFF_REMOVED_BG_HOVER = '#ffa39eaa';
/* eslint-enable rulesdir/no-hardcoded-colors */

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    display: none;
`;

const AddNewDescription = styled(Button)`
    display: none;
    margin: -4px;
    width: 140px;
`;

const ExpandedActions = styled.div`
    height: 10px;
`;

const DescriptionWrapper = styled.span`
    display: inline-flex;
    align-items: center;
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
    min-height: 22px;
    &:hover ${EditIcon} {
        display: inline-block;
    }

    &:hover ${AddNewDescription} {
        display: block;
    }
    & ins.diff {
        background-color: ${DIFF_ADDED_BG};
        text-decoration: none;
        &:hover {
            background-color: ${DIFF_ADDED_BG_HOVER};
        }
    }
    & del.diff {
        background-color: ${DIFF_REMOVED_BG};
        text-decoration: line-through;
        /* original V1 selector typo ("&: hover") preserved intentionally to avoid changing rendered behavior */
        &: hover {
            background-color: ${DIFF_REMOVED_BG_HOVER};
        }
    }
`;
const EditedLabel = styled(Typography.Text)`
    position: absolute;
    right: -10px;
    top: -15px;
    color: ${(props) => props.theme.colors.textTertiary};
    opacity: 0.5;
    font-style: italic;
`;

const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
`;

const AttributeDescription = styled.div`
    margin-top: 8px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const StyledAttributeViewer = styled(Editor)`
    padding-right: 8px;
    display: block;
    .remirror-editor.ProseMirror {
        padding: 0;
        color: ${(props) => props.theme.colors.textTertiary};
    }
`;

const EditButton = styled(Button)`
    margin-left: 4px;
`;

type Props = {
    onBAExpanded?: (expanded: boolean) => void;
    baExpanded?: boolean;
    description: string;
    original?: string | null;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
    isEdited?: boolean;
    isReadOnly?: boolean;
    businessAttributeDescription?: string;
    isPropagated?: boolean;
    sourceDetail?: StringMapEntry[] | null;
};

const ABBREVIATED_LIMIT = 80;

export default function DescriptionField({
    baExpanded,
    onBAExpanded: handleBAExpanded,
    description,
    onUpdate,
    isEdited = false,
    original,
    isReadOnly,
    businessAttributeDescription,
    isPropagated,
    sourceDetail,
}: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [showAddModal, setShowAddModal] = useState(false);
    const isSchemaEditable = React.useContext(SchemaEditableContext) && !isReadOnly;
    const onCloseModal = () => setShowAddModal(false);
    const { urn, entityType } = useEntityData();
    const attributeDescriptionOverLimit = businessAttributeDescription
        ? removeMarkdown(businessAttributeDescription).length > 80
        : false;

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType,
            entityUrn: urn,
        });
    };

    const onUpdateModal = async (desc: string | null) => {
        message.loading({ content: tf('updating') });
        try {
            await onUpdate(desc || '');
            message.destroy();
            message.success({ content: tf('updated'), duration: 2 });
            sendAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error)
                message.error({
                    content: t('fieldDescription.updateFailed', { message: e.message || '' }),
                    duration: 2,
                });
        }
        onCloseModal();
    };

    const showAddDescription = isSchemaEditable && !description;

    return (
        <DescriptionContainer>
            <>
                <DescriptionWrapper>
                    {isPropagated && (
                        <>
                            <PropagationDetails sourceDetail={sourceDetail} />
                            &nbsp;
                        </>
                    )}
                    <CompactMarkdownViewer
                        content={description}
                        lineLimit={2}
                        fixedLineHeight
                        customStyle={{ fontSize: '12px' }}
                        scrollableY={false}
                    />
                    {isSchemaEditable && !!description && (
                        <EditButton
                            icon={{ icon: PencilSimple }}
                            size="md"
                            variant="text"
                            onClick={() => setShowAddModal(true)}
                        />
                    )}
                </DescriptionWrapper>
            </>
            {isEdited && <EditedLabel>{t('fieldDescription.edited')}</EditedLabel>}
            {showAddModal && (
                <div>
                    <UpdateDescriptionModal
                        title={
                            description
                                ? t('fieldDescription.updateDescriptionTitle')
                                : t('fieldDescription.addDescriptionTitle')
                        }
                        description={description}
                        original={original || ''}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                        isAddDesc={!description}
                    />
                </div>
            )}
            {showAddDescription && (
                <AddNewDescription variant="text" onClick={() => setShowAddModal(true)}>
                    {t('fieldDescription.addDescriptionButton')}
                </AddNewDescription>
            )}
            {!!businessAttributeDescription && (
                <>
                    {baExpanded || !attributeDescriptionOverLimit ? (
                        <AttributeDescription>
                            {!!businessAttributeDescription && (
                                <StyledAttributeViewer content={businessAttributeDescription} readOnly />
                            )}
                            {!!businessAttributeDescription && (
                                <ExpandedActions>
                                    {attributeDescriptionOverLimit && (
                                        <ReadLessText
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (handleBAExpanded) {
                                                    handleBAExpanded(false);
                                                }
                                            }}
                                        >
                                            {tc('readLess')}
                                        </ReadLessText>
                                    )}
                                </ExpandedActions>
                            )}
                        </AttributeDescription>
                    ) : (
                        <AttributeDescription>
                            <StripMarkdownText
                                limit={ABBREVIATED_LIMIT}
                                readMore={
                                    <>
                                        <Typography.Link
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (handleBAExpanded) {
                                                    handleBAExpanded(true);
                                                }
                                            }}
                                        >
                                            {tc('readMore')}
                                        </Typography.Link>
                                    </>
                                }
                                shouldWrap
                            >
                                {businessAttributeDescription}
                            </StripMarkdownText>
                        </AttributeDescription>
                    )}
                </>
            )}
        </DescriptionContainer>
    );
}
