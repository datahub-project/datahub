import { Typography, message, Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import CompactMarkdownViewer from '@src/app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { UpdateDatasetMutation } from '../../../../../../graphql/dataset.generated';
import UpdateDescriptionModal from '../../../../shared/components/legacy/DescriptionModal';
import { removeMarkdown } from '../../../../shared/components/styled/StripMarkdownText';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { Editor } from '../../../../shared/tabs/Documentation/components/editor/Editor';
import { REDESIGN_COLORS } from '../../../../shared/constants';
import { StringMapEntry } from '../../../../../../types.generated';
import DocumentationPropagationDetails from '../../../../../sharedV2/propagation/DocumentationPropagationDetails';

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    display: none;
`;

const AddNewDescription = styled(Button)`
    display: flex;
    width: 140px;
    background-color: #fafafa;
    border-radius: 4px;
    align-items: center;
    justify-content: center;
`;

const ExpandedActions = styled.div`
    height: 10px;
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: inline-block;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
    width: 100%;
    min-height: 22px;
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    vertical-align: middle;
    &:hover ${EditIcon} {
        display: inline-block;
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
    display: inline-block;
    margin-left: 8px;
    color: rgba(150, 150, 150, 0.5);
    font-style: italic;
    position: relative;
    top: -2px;
`;

const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
`;

const StyledViewer = styled(Editor)`
    padding-right: 8px;
    display: block;

    .remirror-editor.ProseMirror {
        padding: 0;
        font-size: 12px;
        font-weight: 400;
        line-height: 24px;
        color: ${REDESIGN_COLORS.DARK_GREY};
        vertical-align: middle;
    }
`;

const DescriptionWrapper = styled.span`
    display: inline-flex;
    align-items: center;
`;

const AddModalWrapper = styled.div``;

type Props = {
    onExpanded: (expanded: boolean) => void;
    expanded: boolean;
    description: string;
    fieldPath?: string;
    original?: string | null;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
    handleShowMore?: (_: string) => void;
    isEdited?: boolean;
    isReadOnly?: boolean;
    isPropagated?: boolean;
    sourceDetail?: StringMapEntry[] | null;
};

export default function DescriptionField({
    expanded,
    onExpanded: handleExpanded,
    description,
    fieldPath,
    onUpdate,
    handleShowMore,
    isEdited = false,
    original,
    isReadOnly,
    isPropagated,
    sourceDetail,
}: Props) {
    const [showAddModal, setShowAddModal] = useState(false);

    const overLimit = removeMarkdown(description).length > 40;
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const onCloseModal = () => {
        setShowAddModal(false);
    };
    const { urn, entityType } = useEntityData();

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

    const enableEdits = isSchemaEditable && !isReadOnly;
    const EditButton =
        (enableEdits && description && <EditIcon twoToneColor="#52c41a" onClick={() => setShowAddModal(true)} />) ||
        undefined;

    const showAddButton = enableEdits && !description;

    return (
        <DescriptionContainer>
            {/* {expanded || !overLimit ? ( */}
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
                description && (
                    <>
                        {/* <StripMarkdownText
                        limit={ABBREVIATED_LIMIT}
                        // readMore={
                        //     <>
                        //         <Typography.Link
                        //             onClick={(e) => {
                        //                 e.stopPropagation();
                        //                 handleExpanded(true);
                        //             }}
                        //         >
                        //             Read More
                        //         </Typography.Link>
                        //     </>
                        // }
                        suffix={EditButton}
                        shouldWrap
                    > */}
                        <DescriptionWrapper>
                            {isPropagated && <DocumentationPropagationDetails sourceDetail={sourceDetail} />}
                            &nbsp;
                            <CompactMarkdownViewer
                                content={description}
                                lineLimit={1}
                                handleShowMore={() => handleShowMore && handleShowMore(fieldPath || '')}
                                fixedLineHeight
                                customStyle={{ fontSize: '12px' }}
                                scrollableY={false}
                            />
                        </DescriptionWrapper>
                        {/* </StripMarkdownText> */}
                    </>
                )
            )}
            {isSchemaEditable && isEdited && <EditedLabel>(edited)</EditedLabel>}
            {showAddModal && (
                <AddModalWrapper onClick={(e) => e.stopPropagation()}>
                    <UpdateDescriptionModal
                        title={description ? 'Update description' : 'Add description'}
                        description={description}
                        original={original || ''}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                        isAddDesc={!description}
                    />
                </AddModalWrapper>
            )}
            {showAddButton && (
                <AddNewDescription
                    type="text"
                    onClick={(e) => {
                        setShowAddModal(true);
                        e.stopPropagation();
                    }}
                >
                    Add Description
                </AddNewDescription>
            )}
        </DescriptionContainer>
    );
}
