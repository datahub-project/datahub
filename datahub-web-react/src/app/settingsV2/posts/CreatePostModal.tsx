import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import CreatePostForm, { PostFormData } from '@app/settingsV2/posts/CreatePostForm';
import { PostEntry } from '@app/settingsV2/posts/PostsListColumns';
import handleGraphQLError from '@app/sharedV2/handleGraphQLError';
import { Modal, toast } from '@src/alchemy-components';

import { useCreatePostMutation, useUpdatePostMutation } from '@graphql/mutations.generated';
import { MediaType, PostContentType, PostType, UpdatePostContentInput } from '@types';

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
    showPinnedLinks: boolean;
    onClose: () => void;
    onCreate: () => void;
    onEdit: (urn: string, content: UpdatePostContentInput) => void;
};

export default function CreatePostModal({ onClose, onCreate, editData, onEdit, showPinnedLinks }: Props) {
    const { t } = useTranslation('settings.posts');
    const { t: tc } = useTranslation('common.actions');
    const [createPostMutation] = useCreatePostMutation();
    const [updatePostMutation] = useUpdatePostMutation();
    const [formData, setFormData] = useState<PostFormData>(() => getInitialFormData(editData));

    const handleChange = useCallback((field: keyof PostFormData, value: string | PostContentType) => {
        setFormData((prev) => ({ ...prev, [field]: value }));
    }, []);

    const isValid = useMemo(() => formData.title.trim().length > 0, [formData.title]);

    const isLink = formData.type === PostContentType.Link;
    const content: UpdatePostContentInput = {
        contentType: formData.type,
        title: formData.title,
        description: formData.description || null,
        link: isLink ? formData.link || null : null,
        media: isLink && formData.location ? { type: MediaType.Image, location: formData.location } : null,
    };

    const onCreatePost = () => {
        createPostMutation({
            variables: {
                input: {
                    postType: PostType.HomePageAnnouncement,
                    content,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t(showPinnedLinks ? 'createSuccess' : 'announcement.createSuccess'));
                    onCreate();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t(showPinnedLinks ? 'createError' : 'announcement.createError'),
                    permissionMessage: t(showPinnedLinks ? 'createUnauthorized' : 'announcement.createUnauthorized'),
                });
            });
        onClose();
    };

    const onUpdatePost = () => {
        updatePostMutation({
            variables: {
                input: {
                    urn: editData?.urn,
                    postType: PostType.HomePageAnnouncement,
                    content,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t(showPinnedLinks ? 'updateSuccess' : 'announcement.updateSuccess'));
                    onEdit(editData.urn, content);
                }
            })
            .catch((e) => {
                toast.error(t(showPinnedLinks ? 'updateError' : 'announcement.updateError'));
                console.error('Failed to update Post:', e.message);
            });
        onClose();
    };

    const titleText = editData ? tc('edit') : tc('create');

    return (
        <Modal
            title={titleText}
            open
            onCancel={onClose}
            width={800}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: !editData ? tc('create') : tc('update'),
                    onClick: !editData ? onCreatePost : onUpdatePost,
                    variant: 'filled',
                    disabled: !isValid,
                    buttonDataTestId: !editData ? 'create-post-button' : 'update-post-button',
                },
            ]}
        >
            <CreatePostForm formData={formData} onChange={handleChange} showPinnedLinks={showPinnedLinks} />
        </Modal>
    );
}
