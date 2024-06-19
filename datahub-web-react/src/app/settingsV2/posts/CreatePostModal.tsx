import React, { useEffect, useState } from 'react';
import { Button, Form, message, Modal } from 'antd';
import CreatePostForm from './CreatePostForm';
import {
    CREATE_POST_BUTTON_ID,
    DESCRIPTION_FIELD_NAME,
    LINK_FIELD_NAME,
    LOCATION_FIELD_NAME,
    TYPE_FIELD_NAME,
    TITLE_FIELD_NAME,
} from './constants';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { MediaType, PostContentType, PostType } from '../../../types.generated';
import { useCreatePostMutation, useUpdatePostMutation } from '../../../graphql/mutations.generated';
import { PostEntry } from './PostsListColumns';
import handleGraphQLError from '../../shared/handleGraphQLError';

type Props = {
    editData: PostEntry;
    onClose: () => void;
    onCreate: (
        contentType: string,
        title: string,
        description: string | undefined,
        link: string | undefined,
        location: string | undefined,
    ) => void;
    onEdit: () => void;
};

export default function CreatePostModal({ onClose, onCreate, editData, onEdit }: Props) {
    const [createPostMutation] = useCreatePostMutation();
    const [updatePostMutation] = useUpdatePostMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
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
        const contentTypeValue = form.getFieldValue(TYPE_FIELD_NAME) ?? PostContentType.Text;
        const mediaValue =
            form.getFieldValue(TYPE_FIELD_NAME) && form.getFieldValue(LOCATION_FIELD_NAME)
                ? {
                      type: MediaType.Image,
                      location: form.getFieldValue(LOCATION_FIELD_NAME) ?? null,
                  }
                : null;
        createPostMutation({
            variables: {
                input: {
                    postType: PostType.HomePageAnnouncement,
                    content: {
                        contentType: contentTypeValue,
                        title: form.getFieldValue(TITLE_FIELD_NAME),
                        description: form.getFieldValue(DESCRIPTION_FIELD_NAME) ?? null,
                        link: form.getFieldValue(LINK_FIELD_NAME) ?? null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Created Post!`,
                        duration: 3,
                    });
                    onCreate(
                        form.getFieldValue(TYPE_FIELD_NAME) ?? PostContentType.Text,
                        form.getFieldValue(TITLE_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        form.getFieldValue(LINK_FIELD_NAME),
                        form.getFieldValue(LOCATION_FIELD_NAME),
                    );
                    form.resetFields();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to create Post! An unexpected error occurred',
                    permissionMessage: 'Unauthorized to create Post. Please contact your DataHub administrator.',
                });
            });
        onClose();
    };

    const onUpdatePost = () => {
        const contentTypeValue = form.getFieldValue(TYPE_FIELD_NAME) ?? PostContentType.Text;
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
                    urn: editData?.urn,
                    postType: PostType.HomePageAnnouncement,
                    content: {
                        contentType: contentTypeValue,
                        title: form.getFieldValue(TITLE_FIELD_NAME),
                        description: form.getFieldValue(DESCRIPTION_FIELD_NAME) ?? null,
                        link: form.getFieldValue(LINK_FIELD_NAME) ?? null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Updated Post!`,
                        duration: 3,
                    });
                    onEdit();
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: 'Failed to update Post! An unknown error occured.', duration: 3 });
                console.error('Failed to update Post:', e.message);
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

    const titleText = editData ? 'Edit Post' : 'Create new Post';

    return (
        <Modal
            title={titleText}
            open
            onCancel={onCloseModal}
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        id={CREATE_POST_BUTTON_ID}
                        data-testid={!editData ? 'create-post-button' : 'update-post-button'}
                        onClick={!editData ? onCreatePost : onUpdatePost}
                        disabled={!createButtonEnabled}
                    >
                        {!editData ? 'Create' : 'Update'}
                    </Button>
                </>
            }
        >
            <CreatePostForm
                setCreateButtonEnabled={setCreateButtonEnabled}
                form={form}
                contentType={editData?.contentType as PostContentType}
            />
        </Modal>
    );
}
