import { Typography, message, Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../../graphql/dataset.generated';
import { StringMapEntry } from '../../../../../../types.generated';
import PropagationDetails from '../../../../shared/propagation/PropagationDetails';
import UpdateDescriptionModal from '../../../../shared/components/legacy/DescriptionModal';
import StripMarkdownText, { removeMarkdown } from '../../../../shared/components/styled/StripMarkdownText';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import { useEntityData } from '../../../../shared/EntityContext';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { Editor } from '../../../../shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../../../shared/constants';

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
        background-color: #b7eb8f99;
        text-decoration: none;
        &:hover {
            background-color: #b7eb8faa;
        }
    }
    & del.diff {
        background-color: #ffa39e99;
        text-decoration: line-through;
        &: hover {
            background-color: #ffa39eaa;
        }
    }
`;
const EditedLabel = styled(Typography.Text)`
    position: absolute;
    right: -10px;
    top: -15px;
    color: rgba(150, 150, 150, 0.5);
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
    color: ${ANTD_GRAY[7]};
`;

const StyledAttributeViewer = styled(Editor)`
    padding-right: 8px;
    display: block;
    .remirror-editor.ProseMirror {
        padding: 0;
        color: ${ANTD_GRAY[7]};
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
        message.loading({ content: 'Updating...' });
        try {
            await onUpdate(desc || '');
            message.destroy();
            message.success({ content: 'Updated!', duration: 2 });
            sendAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) message.error({ content: `Update Failed! \n ${e.message || ''}`, duration: 2 });
        }
        onCloseModal();
    };

    const EditButton =
        (isSchemaEditable && description && (
            <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />
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
                                    Read Less
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
                                        Read More
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
            {isEdited && <EditedLabel>(edited)</EditedLabel>}
            {showAddModal && (
                <div>
                    <UpdateDescriptionModal
                        title={description ? 'Update description' : 'Add description'}
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
                    + Add Description
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
                                        Read Less
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
                                        Read More
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
