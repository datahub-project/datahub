import { Typography, message, Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { UpdateDatasetMutation } from '../../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../../../../shared/components/legacy/DescriptionModal';
import StripMarkdownText, { removeMarkdown } from '../../../../shared/components/styled/StripMarkdownText';
import MarkdownViewer from '../../../../shared/components/legacy/MarkdownViewer';

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

const DescriptionText = styled(MarkdownViewer)`
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
    description: string;
    original?: string | null;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
    editable?: boolean;
    isEdited?: boolean;
};

const ABBREVIATED_LIMIT = 80;

export default function DescriptionField({
    description,
    onUpdate,
    editable = true,
    isEdited = false,
    original,
}: Props) {
    const [showAddModal, setShowAddModal] = useState(false);
    const overLimit = removeMarkdown(description).length > 80;
    const [expanded, setExpanded] = useState(!overLimit);

    const onCloseModal = () => setShowAddModal(false);

    const onUpdateModal = async (desc: string | null) => {
        message.loading({ content: 'Updating...' });
        try {
            await onUpdate(desc || '');
            message.destroy();
            message.success({ content: 'Updated!', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) message.error({ content: `Update Failed! \n ${e.message || ''}`, duration: 2 });
        }
        onCloseModal();
    };

    const EditButton =
        (editable && description && <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />) ||
        undefined;

    const showAddDescription = editable && !description;

    return (
        <DescriptionContainer
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
            }}
        >
            {expanded ? (
                <>
                    {!!description && <DescriptionText source={description} />}
                    {!!description && (
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
                        {description}
                    </StripMarkdownText>
                </>
            )}
            {editable && isEdited && <EditedLabel>(edited)</EditedLabel>}
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
        </DescriptionContainer>
    );
}
