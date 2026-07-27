import { EditOutlined } from '@ant-design/icons';
import { FetchResult } from '@apollo/client';
import { Button, Typography, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import StripMarkdownText, { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';
import PropagationDetails from '@app/entity/shared/propagation/PropagationDetails';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { UpdateDatasetMutation } from '@graphql/dataset.generated';
import { StringMapEntry } from '@types';

// Legacy V1 decorative colors. The diff-highlight greens/reds and the edit-icon green are
// visually distinct from the semantic theme tokens (bgSurfaceSuccess/Error, iconSuccess would
// render a much paler / different shade), so they are preserved verbatim to keep the original
// appearance of this deprecated component.
/* eslint-disable rulesdir/no-hardcoded-colors -- (legacy-color) preserve original V1 appearance; semantic tokens differ visually */
const EDIT_ICON_TWO_TONE_COLOR = '#52c41a';
const DIFF_ADDED_BG = '#b7eb8f99';
const DIFF_ADDED_BG_HOVER = '#b7eb8faa';
const DIFF_REMOVED_BG = '#ffa39e99';
const DIFF_REMOVED_BG_HOVER = '#ffa39eaa';
const EDITED_LABEL_COLOR = 'rgba(150, 150, 150, 0.5)';
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
    color: ${EDITED_LABEL_COLOR};
    font-style: italic;
`;

const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
`;

const StyledViewer = styled(Editor)`
    padding-right: 8px;
    display: block;

    .remirror-editor.ProseMirror {
        padding: 0;
    }
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

type Props = {
    onExpanded: (expanded: boolean) => void;
    onBAExpanded?: (expanded: boolean) => void;
    expanded: boolean;
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
    expanded,
    baExpanded,
    onExpanded: handleExpanded,
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
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [showAddModal, setShowAddModal] = useState(false);
    const overLimit = removeMarkdown(description).length > 80;
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
                    content: t('dataset.updateDescriptionError', { error: e.message || '' }),
                    duration: 2,
                });
        }
        onCloseModal();
    };

    const EditButton =
        (isSchemaEditable && description && (
            <EditIcon twoToneColor={EDIT_ICON_TWO_TONE_COLOR} onClick={() => setShowAddModal(true)} />
        )) ||
        undefined;

    const showAddDescription = isSchemaEditable && !description;

    return (
        <DescriptionContainer>
            {expanded ? (
                <>
                    {!!description && <StyledViewer content={description} readOnly />}
                    {!!description && (EditButton || overLimit) && (
                        <ExpandedActions>
                            {overLimit && (
                                <ReadLessText
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        handleExpanded(false);
                                    }}
                                >
                                    {tc('readLess')}
                                </ReadLessText>
                            )}
                            {EditButton}
                        </ExpandedActions>
                    )}
                </>
            ) : (
                <>
                    <DescriptionWrapper>
                        {isPropagated && <PropagationDetails sourceDetail={sourceDetail} />}
                        &nbsp;
                        <StripMarkdownText
                            limit={ABBREVIATED_LIMIT}
                            readMore={
                                <>
                                    <Typography.Link
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            handleExpanded(true);
                                        }}
                                    >
                                        {tc('readMore')}
                                    </Typography.Link>
                                </>
                            }
                            suffix={EditButton}
                            shouldWrap
                        >
                            {description}
                        </StripMarkdownText>
                    </DescriptionWrapper>
                </>
            )}
            {isEdited && <EditedLabel>{t('dataset.editedLabel')}</EditedLabel>}
            {showAddModal && (
                <div>
                    <UpdateDescriptionModal
                        title={description ? t('dataset.updateDescriptionTitle') : t('dataset.addDescriptionTitle')}
                        description={description}
                        original={original || ''}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                        isAddDesc={!description}
                    />
                </div>
            )}
            {showAddDescription && (
                <AddNewDescription type="text" onClick={() => setShowAddModal(true)}>
                    {t('dataset.addDescriptionButton')}
                </AddNewDescription>
            )}
            <AttributeDescription>
                {baExpanded || !attributeDescriptionOverLimit ? (
                    <>
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
                    </>
                ) : (
                    <>
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
                    </>
                )}
            </AttributeDescription>
        </DescriptionContainer>
    );
}
