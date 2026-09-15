import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';

import PostItemMenu from '@app/settingsV2/posts/PostItemMenu';

export interface PostEntry {
    urn: string;
    title: string;
    contentType: string;
    description?: Maybe<string> | string;
    link?: string | null;
    imageUrl?: string;
}

export function PostListMenuColumn(handleDelete: (urn: string) => void, handleEdit: (urn: PostEntry) => void) {
    return (record: PostEntry) => (
        <PostItemMenu
            title={record.title}
            urn={record.urn}
            onDelete={() => handleDelete(record.urn)}
            onEdit={() => handleEdit(record)}
        />
    );
}
