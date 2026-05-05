import React, { useCallback, useMemo, useState } from 'react';

import CreatePostForm, { PostFormData } from '@app/settingsV2/posts/CreatePostForm';
import { PostEntry } from '@app/settingsV2/posts/PostsListColumns';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { Modal, toast } from '@src/alchemy-components';

import { useCreatePostMutation, useUpdatePostMutation } from '@graphql/mutations.generated';
import { MediaType, PostContentType, PostType } from '@types';

function getInitialFormData(editData?: PostEntry): PostFormData {
    if (editData) {
        return {
            type: (editData.contentType as PostContentType) || PostContentType.Text,
            title: editData.title || '',
            description: (editData.description as string) || '',
            link: editData.link || '',
            location: editData.imageUrl || '',
        };
    }
    return {
        type: PostContentType.Text,
        title: '',
        description: '',
        link: '',
        location: '',
    };
}

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
    const [formData, setFormData] = useState<PostFormData>(() => getInitialFormData(editData));

    const handleChange = useCallback((field: keyof PostFormData, value: string | PostContentType) => {
        setFormData((prev) => ({ ...prev, [field]: value }));
    }, []);

    const isValid = useMemo(() => formData.title.trim().length > 0, [formData.title]);

    const onCreatePost = () => {
        const mediaValue =
            formData.type && formData.location ? { type: MediaType.Image, location: formData.location } : null;
        createPostMutation({
            variables: {
                input: {
                    postType: PostType.HomePageAnnouncement,
                    content: {
                        contentType: formData.type || PostContentType.Text,
                        title: formData.title,
                        description: formData.description || null,
                        link: formData.link || null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Created Post!');
                    onCreate(
                        formData.type || PostContentType.Text,
                        formData.title,
                        formData.description,
                        formData.link,
                        formData.location,
                    );
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
        const mediaValue =
            formData.type && formData.location ? { type: MediaType.Image, location: formData.location } : null;
        updatePostMutation({
            variables: {
                input: {
                    urn: editData?.urn,
                    postType: PostType.HomePageAnnouncement,
                    content: {
                        contentType: formData.type || PostContentType.Text,
                        title: formData.title,
                        description: formData.description || null,
                        link: formData.link || null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success('Updated Post!');
                    onEdit();
                }
            })
            .catch((e) => {
                toast.error('Failed to update Post! An unknown error occurred.');
                console.error('Failed to update Post:', e.message);
            });
        onClose();
    };

    const titleText = editData ? 'Edit' : 'Create';

    return (
        <Modal
            title={titleText}
            open
            onCancel={onClose}
            width={800}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: !editData ? 'Create' : 'Update',
                    onClick: !editData ? onCreatePost : onUpdatePost,
                    variant: 'filled',
                    disabled: !isValid,
                    buttonDataTestId: !editData ? 'create-post-button' : 'update-post-button',
                },
            ]}
        >
            <CreatePostForm formData={formData} onChange={handleChange} />
        </Modal>
    );
}
