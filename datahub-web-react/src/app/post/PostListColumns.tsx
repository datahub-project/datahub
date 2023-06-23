import React from 'react';
import { Typography } from 'antd';
// import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Maybe } from '../../types.generated';
import PostItemMenu from './PostItemMenu';

interface PostEntry {
    title: string;
    contentType: string;
    description: Maybe<string>;
    urn: string;
}

const PostTitleContainer = styled.div`
    margin-left: 16px;
    margin-right: 16px;
    display: inline;
`;

export function PostListMenuColumn(handleDelete: (urn: string) => void) {
    return (record: PostEntry) => (
        <PostItemMenu title={record.title} urn={record.urn} onDelete={() => handleDelete(record.urn)} />
    );
}

export function PostTitleColumn() {
    return (record: PostEntry) => (
        <span data-testid={record.urn}>
            <PostTitleContainer>
                <Typography.Text>{record.title}</Typography.Text>
            </PostTitleContainer>
        </span>
    );
}

export function PostDescriptionColumn() {
    return (record: PostEntry) => (
        <span>
            <PostTitleContainer>
                <Typography.Text>{record.description}</Typography.Text>
            </PostTitleContainer>
        </span>
    );
}

export function PostContentTypeColumn() {
    return (record: PostEntry) => (
        <span>
            <PostTitleContainer>
                <Typography.Text>{record.contentType}</Typography.Text>
            </PostTitleContainer>
        </span>
    );
}
