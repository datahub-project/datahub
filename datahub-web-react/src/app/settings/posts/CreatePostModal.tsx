import React, { useState } from 'react';
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
import { useCreatePostMutation } from '../../../graphql/mutations.generated';

type Props = {
    onClose: () => void;
    onCreate: (
        contentType: string,
        title: string,
        description: string | undefined,
        link: string | undefined,
        location: string | undefined,
    ) => void;
};

export default function CreatePostModal({ onClose, onCreate }: Props) {
    const [createPostMutation] = useCreatePostMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();
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
            .catch((e) => {
                message.destroy();
                message.error({ content: 'Failed to create Post! An unknown error occured.', duration: 3 });
                console.error('Failed to create Post:', e.message);
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createPostButton',
    });

    return (
        <Modal
            title="Create new Post"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button
                        id={CREATE_POST_BUTTON_ID}
                        data-testid="create-post-button"
                        onClick={onCreatePost}
                        disabled={!createButtonEnabled}
                    >
                        Create
                    </Button>
                </>
            }
        >
            <CreatePostForm setCreateButtonEnabled={setCreateButtonEnabled} form={form} />
        </Modal>
    );
}
