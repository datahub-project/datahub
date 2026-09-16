import { Form, Input, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { PostEntry } from '@app/settings/posts/PostsListColumns';
import {
    CREATE_POST_BUTTON_ID,
    LINK_FIELD_NAME,
    LOCATION_FIELD_NAME,
    TYPE_FIELD_NAME,
} from '@app/settings/posts/constants';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Editor, Modal } from '@src/alchemy-components';

import { useCreatePostMutation, useUpdatePostMutation } from '@graphql/mutations.generated';
import { MediaType, PostContentType, PostType, SubResourceType } from '@types';

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
    border: 1px solid ${(props) => props.theme.colors.border};
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

export default function CreateEntityAnnouncementModal({
    urn,
    subResource,
    onClose,
    onCreate,
    editData,
    onEdit,
}: Props) {
    const { t } = useTranslation('entity.shared.profile');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');
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
                        content: t('noteModal.createdSuccess'),
                        duration: 3,
                    });
                    onCreate?.(form.getFieldValue(noteTitle), form.getFieldValue(noteContent));
                    form.resetFields();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t('noteModal.createError'),
                    permissionMessage: t('noteModal.createUnauthorized'),
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
                        content: t('noteModal.updatedSuccess'),
                        duration: 3,
                    });
                    onEdit?.();
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('noteModal.updateError'), duration: 3 });
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
    const titleText = editData ? t('noteModal.editTitle') : t('noteModal.createTitle');

    /**
     * Handles the change in the description field.
     * @param {string} value - The new value of the description field.
     */
    const handleDescriptionChange = (value) => {
        form.setFieldsValue({ [noteContent]: value });
        form.validateFields();
    };

    return (
        <Modal
            title={titleText}
            open
            onCancel={onCloseModal}
            width={650}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                    id: 'createPostButton',
                },
                {
                    text: !editData ? tc('create') : tc('update'),
                    onClick: !editData ? onCreatePost : onUpdatePost,
                    variant: 'filled',
                    disabled: !createButtonEnabled,
                    id: CREATE_POST_BUTTON_ID,
                    buttonDataTestId: !editData ? 'create-announcement-button' : 'update-announcement-button',
                },
            ]}
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
                <Typography.Paragraph>{tcl('title')}</Typography.Paragraph>
                <SubFormItem name={noteTitle} rules={[{ min: 1, max: 500, required: true }]} hasFeedback>
                    <Input placeholder={t('noteModal.titlePlaceholder')} />
                </SubFormItem>
                <Typography.Paragraph>{t('noteModal.contentLabel')}</Typography.Paragraph>
                <SubFormItem name={noteContent} rules={[{ min: 1, max: 5000 }]} hasFeedback>
                    <EditorContainer>
                        <Editor
                            content={editData?.description || ''}
                            onChange={handleDescriptionChange}
                            placeholder={t('noteModal.contentPlaceholder')}
                        />
                    </EditorContainer>
                </SubFormItem>
            </Form>
        </Modal>
    );
}
