import React, { useEffect, useState } from 'react';
import { Form, Input, Modal, Typography, message } from 'antd';
import styled from 'styled-components';
import { Button, colors } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { useCreatePostMutation, useUpdatePostMutation } from '../../../../graphql/mutations.generated';
import { MediaType, PostContentType, PostType, SubResourceType } from '../../../../types.generated';
import { PostEntry } from '../../../settings/posts/PostsListColumns';
import {
    CREATE_POST_BUTTON_ID,
    LINK_FIELD_NAME,
    LOCATION_FIELD_NAME,
    TYPE_FIELD_NAME,
} from '../../../settings/posts/constants';
import handleGraphQLError from '../../../shared/handleGraphQLError';
import { useEnterKeyListener } from '../../../shared/useEnterKeyListener';
import { Editor } from '../tabs/Documentation/components/editor/Editor';

const SubFormItem = styled(Form.Item)`
    margin-bottom: 24px;
`;

type Props = {
    urn: string;
    subResource?: string | null;
    editData?: PostEntry;
    onClose: () => void;
    onCreate?: (title: string, description: string | undefined) => void;
    onEdit?: () => void;
};

const EditorContainer = styled.div`
    flex: 1;
    border: 1px solid #d9d9d9;
    border-radius: 8px;
    .remirror-editor.ProseMirror {
        padding: 10px;
        min-height: 150px;
    }
    .remirror-theme > div:first-child {
        padding: 0;
        margin: 0;
    }
`;

const ModalHeaderContainer = styled.div`
    display: flex;
    align-items: center;
    font-size: 16px;
    color: ${colors.gray[600]}
    font-weight: bold; 
    font-family: 'Mulish', sans-serif;
    font-weight: lighter;
`;

const ModalTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const StyledModal = styled(Modal)`
    width: 680px !important;
`;

export default function CreateEntityAnnouncementModal({
    urn,
    subResource,
    onClose,
    onCreate,
    editData,
    onEdit,
}: Props) {
    const [createPostMutation] = useCreatePostMutation();
    const [updatePostMutation] = useUpdatePostMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const noteTitle = 'title';
    const noteContent = 'description';
    const [form] = Form.useForm();

    useEffect(() => {
        if (editData) {
            form.setFieldsValue({
                description: editData.description,
                title: editData.title,
                link: editData.link,
                location: editData.imageUrl,
                type: editData.contentType,
            });
        }
    }, [editData, form]);

    const onCreatePost = () => {
        createPostMutation({
            variables: {
                input: {
                    postType: PostType.EntityAnnouncement,
                    content: {
                        contentType: PostContentType.Text,
                        title: form.getFieldValue(noteTitle),
                        description: form.getFieldValue(noteContent) ?? null,
                    },
                    resourceUrn: urn,
                    subResource,
                    subResourceType: SubResourceType.DatasetField,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Created Note!`,
                        duration: 3,
                    });
                    onCreate?.(form.getFieldValue(noteTitle), form.getFieldValue(noteContent));
                    form.resetFields();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to create Note! An unexpected error occurred',
                    permissionMessage: 'Unauthorized to create Note. Please contact your DataHub administrator.',
                });
            });
        onClose();
    };

    const onUpdatePost = () => {
        const mediaValue =
            form.getFieldValue(TYPE_FIELD_NAME) && form.getFieldValue(LOCATION_FIELD_NAME)
                ? {
                      type: MediaType.Image,
                      location: form.getFieldValue(LOCATION_FIELD_NAME) ?? null,
                  }
                : null;
        updatePostMutation({
            variables: {
                input: {
                    urn: editData?.urn || '',
                    postType: PostType.EntityAnnouncement,
                    content: {
                        contentType: PostContentType.Text,
                        title: form.getFieldValue(noteTitle),
                        description: form.getFieldValue(noteContent) ?? null,
                        link: form.getFieldValue(LINK_FIELD_NAME) ?? null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Updated Note!`,
                        duration: 3,
                    });
                    onEdit?.();
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: 'Failed to update Note! An unknown error occured.', duration: 3 });
                console.error('Failed to update Note:', e.message);
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createPostButton',
    });

    const onCloseModal = () => {
        form.resetFields();
        onClose();
    };

    // note-- edit announcement functionality is not implemented
    const titleText = editData ? 'Edit Note' : 'Create Note';

    /**
     * Handles the change in the description field.
     * @param {string} value - The new value of the description field.
     */
    const handleDescriptionChange = (value) => {
        form.setFieldsValue({ [noteContent]: value });
        form.validateFields();
    };

    return (
        <StyledModal
            title={
                <ModalHeaderContainer>
                    <ModalTitle>{titleText}</ModalTitle>
                </ModalHeaderContainer>
            }
            open
            onCancel={onCloseModal}
            footer={
                <ModalButtonContainer>
                    <Button onClick={onCloseModal} variant="text" id="createPostButton">
                        Cancel
                    </Button>
                    <Button
                        id={CREATE_POST_BUTTON_ID}
                        data-testid={!editData ? 'create-announcement-button' : 'update-announcement-button'}
                        onClick={!editData ? onCreatePost : onUpdatePost}
                        disabled={!createButtonEnabled}
                    >
                        {!editData ? 'Create' : 'Update'}
                    </Button>
                </ModalButtonContainer>
            }
        >
            <Form
                form={form}
                initialValues={editData}
                layout="vertical"
                onFieldsChange={() => {
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0));
                }}
                onKeyDown={(e) => {
                    // Preventing the modal from closing when the Enter key is pressed
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                    }
                }}
            >
                <Typography.Paragraph>Title</Typography.Paragraph>
                <SubFormItem name={noteTitle} rules={[{ min: 1, max: 500, required: true }]} hasFeedback>
                    <Input placeholder="Your title" />
                </SubFormItem>
                <Typography.Paragraph>Content</Typography.Paragraph>
                <SubFormItem name={noteContent} rules={[{ min: 1, max: 5000 }]} hasFeedback>
                    <EditorContainer>
                        <Editor
                            content={editData?.description || ''}
                            onChange={handleDescriptionChange}
                            placeholder="Write a note. Tag @user or reference @asset to make your note come to life!"
                        />
                    </EditorContainer>
                </SubFormItem>
            </Form>
        </StyledModal>
    );
}
