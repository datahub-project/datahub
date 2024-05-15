import React from 'react';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import PostItemMenu from './PostItemMenu';

export interface PostEntry {
    urn: string;
    title: string;
    contentType: string;
    description?: Maybe<string>;
    link?: string | null;
    imageUrl?: string;
}

const PostText = styled.div<{ minWidth?: number }>`
    ${(props) => props.minWidth !== undefined && `min-width: ${props.minWidth}px;`}
`;

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

export function PostColumn(text: string, minWidth?: number) {
    return <PostText minWidth={minWidth}>{text}</PostText>;
}
