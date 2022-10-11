import { Typography, message, Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../../../../shared/components/legacy/DescriptionModal';
import StripMarkdownText, { removeMarkdown } from '../../../../shared/components/styled/StripMarkdownText';
import MarkdownViewer from '../../../../shared/components/legacy/MarkdownViewer';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import { useEntityData } from '../../../../shared/EntityContext';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    display: none;
`;

const AddNewLabel = styled(Button)`
    display: none;
    margin: -4px;
    width: 140px;
`;

const ExpandedActions = styled.div`
    height: 10px;
`;

const LabelContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
    min-height: 22px;
    &:hover ${EditIcon} {
        display: inline-block;
    }

    &:hover ${AddNewLabel} {
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

const LabelText = styled(MarkdownViewer)`
    padding-right: 8px;
    display: block;
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

type Props = {
    label: string;
    original?: string | null;
    onUpdate: (
        label: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
    isEdited?: boolean;
};

const ABBREVIATED_LIMIT = 40;

export default function LabelField({ label, onUpdate, isEdited = false, original }: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    const overLimit = removeMarkdown(label).length > 80;
    const [expanded, setExpanded] = useState(!overLimit);
    const isSchemaEditableFromContext = React.useContext(SchemaEditableContext);
    const onCloseModal = () => setShowAddModal(false);
    const { urn, entityType, entityData } = useEntityData();

    const user = useGetAuthenticatedUser();
    const ownerExists = entityData?.ownership?.owners?.some(
        (owner) => (owner?.owner?.urn as string) === (user?.corpUser?.urn as string),
    );
    const isSchemaEditable = ownerExists ? isSchemaEditableFromContext : false;

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType,
            entityUrn: urn,
        });
    };

    const onUpdateModal = async (newLabel: string | null) => {
        message.loading({ content: 'Updating...' });
        try {
            await onUpdate(newLabel || '');
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
        (isSchemaEditable && label && <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />) ||
        undefined;

    const showAddLabel = isSchemaEditable && !label;
    const displayLabel = `<b>Business Label:</b> ${label}`;

    return (
        <LabelContainer>
            {expanded ? (
                <>
                    {!!label && <LabelText source={displayLabel} />}
                    {!!label && (
                        <ExpandedActions>
                            {overLimit && (
                                <ReadLessText
                                    onClick={() => {
                                        setExpanded(false);
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
                    <StripMarkdownText
                        limit={ABBREVIATED_LIMIT}
                        readMore={
                            <>
                                <Typography.Link
                                    onClick={() => {
                                        setExpanded(true);
                                    }}
                                >
                                    Read More
                                </Typography.Link>
                            </>
                        }
                        suffix={EditButton}
                    >
                        {displayLabel}
                    </StripMarkdownText>
                </>
            )}

            {isSchemaEditable && isEdited && <EditedLabel>(edited)</EditedLabel>}
            {showAddModal && (
                <div>
                    <UpdateDescriptionModal
                        title={label ? 'Update business label' : 'Add business label'}
                        description={label}
                        original={original || ''}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                        isAddDesc={!label}
                        useTextEditor
                    />
                </div>
            )}
            {showAddLabel && (
                <AddNewLabel type="text" onClick={() => setShowAddModal(true)}>
                    + Add Business Label
                </AddNewLabel>
            )}
        </LabelContainer>
    );
}
