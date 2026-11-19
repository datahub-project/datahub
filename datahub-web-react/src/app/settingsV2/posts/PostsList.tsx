import { Note } from '@phosphor-icons/react/dist/csr/Note';
import removeMd from '@tommoor/remove-markdown';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import CreatePostModal from '@app/settingsV2/posts/CreatePostModal';
import { PostEntry, PostListMenuColumn } from '@app/settingsV2/posts/PostsListColumns';
import { addToListPostCache, removeFromListPostCache } from '@app/settingsV2/posts/utils';
import { scrollToTop } from '@app/sharedV2/searchUtils';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';
import { Alert, EmptyState, OverflowText, Pagination, SearchBar, Table } from '@src/alchemy-components';
import { Column } from '@src/alchemy-components/components/Table/types';

import { useListPostsQuery } from '@graphql/post.generated';
import { PostContentType } from '@types';

const PageContainer = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const PostsContainer = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 16px;
    overflow: hidden;
`;

const TableContainer = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const PaginationContainer = styled.div`
    padding-top: 8px;
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 10;
// Content type isn't indexed for search, so pinned link posts can't be excluded server-side. Load every
// home page post at once and paginate locally so link posts don't throw off page sizes and totals.
const MAX_POSTS = 1000;

type PostListProps = {
    isCreatingPost: boolean;
    setIsCreatingPost: (value: boolean) => void;
};

export const PostList = ({ isCreatingPost, setIsCreatingPost }: PostListProps) => {
    const { t } = useTranslation('settings.posts');
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [editData, setEditData] = useState<PostEntry | undefined>(undefined);

    const pageSize = DEFAULT_PAGE_SIZE;

    const { loading, error, data, client, refetch } = useListPostsQuery({
        variables: {
            input: {
                start: 0,
                count: MAX_POSTS,
                query,
                orFilters: getHomePagePostsFilters(),
            },
        },
        fetchPolicy: query && query.length > 0 ? 'no-cache' : 'cache-first',
    });

    const posts = (data?.listPosts?.posts || []).filter((post) => post.content.contentType === PostContentType.Text);
    const totalPosts = posts.length;
    const lastPage = Math.max(1, Math.ceil(totalPosts / pageSize));

    // Deleting the only post on the last page would otherwise leave an empty page.
    useEffect(() => {
        if (page > lastPage) setPage(lastPage);
    }, [page, lastPage]);

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListPostCache(client, urn, 1, MAX_POSTS);
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
            title: t('columnTitle'),
            key: 'title',
            sorter: (a, b) => a.title.localeCompare(b.title),
            render: (record) => record.title,
            width: '25%',
        },
        {
            title: t('columnDescription'),
            key: 'description',
            render: (record) => <OverflowText text={removeMd(record.description || '')} />,
            width: '55%',
        },
        {
            title: '',
            key: 'menu',
            width: '5%',
            alignment: 'right',
            render: PostListMenuColumn(handleDelete, handleEdit),
        },
    ];

    const tableData: PostEntry[] = posts.slice((page - 1) * pageSize, page * pageSize).map((post) => ({
        urn: post.urn,
        title: post.content.title,
        description: post.content.description || '',
        contentType: post.content.contentType,
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
        return <EmptyState icon={Note} title={t('emptyTitle')} description={t('emptyDescription')} />;
    };

    return (
        <PageContainer>
            {error && <Alert variant="error" title={t('loadError')} />}
            <PostsContainer>
                <SearchBar
                    placeholder={t('searchPlaceholder')}
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
                    <PaginationContainer>
                        <Pagination
                            currentPage={page}
                            itemsPerPage={pageSize}
                            total={totalPosts}
                            onPageChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </PaginationContainer>
                )}
                {isCreatingPost && (
                    <CreatePostModal
                        editData={editData as PostEntry}
                        onClose={handleClose}
                        onEdit={() => setTimeout(() => refetch(), 2000)}
                        onCreate={(title, description) => {
                            addToListPostCache(
                                client,
                                {
                                    properties: {
                                        title,
                                        description: description || null,
                                    },
                                },
                                MAX_POSTS,
                            );
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                )}
            </PostsContainer>
        </PageContainer>
    );
};
