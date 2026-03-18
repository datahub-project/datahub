import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import CreatePostModal from '@app/settingsV2/posts/CreatePostModal';
import { PostEntry, PostListMenuColumn } from '@app/settingsV2/posts/PostsListColumns';
import { POST_TYPE_TO_DISPLAY_TEXT } from '@app/settingsV2/posts/constants';
import { addToListPostCache, removeFromListPostCache } from '@app/settingsV2/posts/utils';
import { scrollToTop } from '@app/shared/searchUtils';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';
import { Alert, EmptyState, OverflowText, Pagination, Pill, SearchBar, Table } from '@src/alchemy-components';
import { Column } from '@src/alchemy-components/components/Table/types';

import { useListPostsQuery } from '@graphql/post.generated';
import { PostContentType } from '@types';

function stripMarkdown(text: string): string {
    return text
        .replace(/<[^>]*>/g, '') // HTML tags
        .replace(/#{1,6}\s?/g, '') // headings
        .replace(/(\*{1,2}|_{1,2}|~{1,2})/g, '') // bold, italic, strikethrough
        .replace(/`{1,3}/g, '') // code
        .replace(/^\s*[-*+]\s+/gm, '') // list markers
        .replace(/^\s*\d+\.\s+/gm, '') // ordered list markers
        .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1') // links
        .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1') // images
        .trim();
}

const PostsContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
`;

const TableContainer = styled.div`
    flex: 1;
    min-height: 0;
    overflow: auto;
`;

const DEFAULT_PAGE_SIZE = 10;

type PostListProps = {
    isCreatingPost: boolean;
    setIsCreatingPost: (value: boolean) => void;
};

export const PostList = ({ isCreatingPost, setIsCreatingPost }: PostListProps) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [editData, setEditData] = useState<PostEntry | undefined>(undefined);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, client, refetch } = useListPostsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
                orFilters: getHomePagePostsFilters(),
            },
        },
        fetchPolicy: query && query.length > 0 ? 'no-cache' : 'cache-first',
    });

    const totalPosts = data?.listPosts?.total || 0;
    const posts = data?.listPosts?.posts || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListPostCache(client, urn, page, pageSize);
        setTimeout(() => {
            refetch?.();
        }, 2000);
    };

    const handleEdit = (post: PostEntry) => {
        setEditData(post);
        setIsCreatingPost(true);
    };

    const handleClose = () => {
        setEditData(undefined);
        setIsCreatingPost(false);
    };

    const allColumns: Column<PostEntry>[] = [
        {
            title: 'Title',
            key: 'title',
            sorter: (a, b) => a.title.localeCompare(b.title),
            render: (record) => record.title,
            width: '25%',
        },
        {
            title: 'Description',
            key: 'description',
            render: (record) => <OverflowText text={stripMarkdown(record.description || '')} />,
            width: '40%',
        },
        {
            title: 'Type',
            key: 'type',
            render: (record) => (
                <Pill
                    label={POST_TYPE_TO_DISPLAY_TEXT[record.contentType]}
                    variant="filled"
                    color={record.contentType === PostContentType.Link ? 'blue' : 'violet'}
                    size="md"
                />
            ),
            minWidth: '100px',
            width: '15%',
        },
        {
            title: '',
            key: 'menu',
            width: '5%',
            alignment: 'right',
            render: PostListMenuColumn(handleDelete, handleEdit),
        },
    ];

    const tableData: PostEntry[] = posts.map((post) => ({
        urn: post.urn,
        title: post.content.title,
        description: post.content.description || '',
        contentType: post.content.contentType,
        link: post.content.link,
        imageUrl: post.content.media?.location,
    }));

    const renderContent = () => {
        if (loading && !data) {
            return (
                <Table
                    columns={allColumns}
                    data={[]}
                    rowKey="urn"
                    showHeader
                    isLoading
                    isScrollable
                    style={{ tableLayout: 'fixed' }}
                />
            );
        }
        if (tableData.length > 0) {
            return (
                <Table
                    columns={allColumns}
                    data={tableData}
                    rowKey="urn"
                    showHeader
                    isScrollable
                    style={{ tableLayout: 'fixed' }}
                />
            );
        }
        return <EmptyState icon="Note" title="No Posts" description="Create a new post to get started." />;
    };

    return (
        <>
            {error && <Alert variant="error" title="Failed to load Posts! An unexpected error occurred." />}
            <PostsContainer>
                <SearchBar
                    placeholder="Search posts..."
                    value={query || ''}
                    onChange={(value) => {
                        setPage(1);
                        setQuery(value || undefined);
                    }}
                    width="250px"
                    allowClear
                />
                <TableContainer>{renderContent()}</TableContainer>
                {totalPosts > pageSize && (
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={totalPosts}
                        onPageChange={onChangePage}
                        showSizeChanger={false}
                    />
                )}
                {isCreatingPost && (
                    <CreatePostModal
                        editData={editData as PostEntry}
                        onClose={handleClose}
                        onEdit={() => setTimeout(() => refetch(), 2000)}
                        onCreate={(urn, title, description) => {
                            addToListPostCache(
                                client,
                                {
                                    urn,
                                    properties: {
                                        title,
                                        description: description || null,
                                    },
                                },
                                pageSize,
                            );
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                )}
            </PostsContainer>
        </>
    );
};
