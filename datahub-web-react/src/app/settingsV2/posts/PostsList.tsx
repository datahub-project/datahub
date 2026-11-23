import { Note } from '@phosphor-icons/react/dist/csr/Note';
import removeMd from '@tommoor/remove-markdown';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';
import CreatePostModal from '@app/settingsV2/posts/CreatePostModal';
import { PostEntry, PostListMenuColumn } from '@app/settingsV2/posts/PostsListColumns';
import { POST_TYPE_TO_DISPLAY_TEXT } from '@app/settingsV2/posts/constants';
import { clearListPostsCache, removeFromListPostsCache, updateListPostsCache } from '@app/settingsV2/posts/utils';
import { scrollToTop } from '@app/sharedV2/searchUtils';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';
import { Alert, EmptyState, OverflowText, Pagination, Pill, SearchBar, Table } from '@src/alchemy-components';
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
// Search indexing lags behind the write, so wait before reloading the list after a create or delete.
const REFRESH_AFTER_WRITE_DELAY_MS = 3000;

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

    // The redesigned home page shows pinned links through Quick Link modules, so this page only manages
    // announcements there.
    const showPinnedLinks = !useShowHomePageRedesign();

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, client } = useListPostsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
                orFilters: getHomePagePostsFilters(showPinnedLinks ? undefined : PostContentType.Text),
            },
        },
        // Cached so edits and deletes can update every page and search result in place.
        fetchPolicy: 'cache-first',
    });

    const totalPosts = data?.listPosts?.total || 0;
    const posts = data?.listPosts?.posts || [];
    const lastPage = Math.max(1, Math.ceil(totalPosts / pageSize));

    // Deleting the only post on the last page would otherwise leave an empty page.
    useEffect(() => {
        if (data && page > lastPage) setPage(lastPage);
    }, [data, page, lastPage]);

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeFromListPostsCache(client, urn);
        // Pages are server slices, so reload them to backfill this page and fix totals on the others.
        setTimeout(() => clearListPostsCache(client), REFRESH_AFTER_WRITE_DELAY_MS);
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
            width: showPinnedLinks ? '40%' : '55%',
        },
        ...(showPinnedLinks
            ? [
                  {
                      title: t('columnType'),
                      key: 'type',
                      render: (record: PostEntry) => (
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
              ]
            : []),
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
        return (
            <EmptyState
                icon={Note}
                title={t(showPinnedLinks ? 'emptyTitle' : 'announcement.emptyTitle')}
                description={t(showPinnedLinks ? 'emptyDescription' : 'announcement.emptyDescription')}
            />
        );
    };

    return (
        <PageContainer>
            {error && <Alert variant="error" title={t(showPinnedLinks ? 'loadError' : 'announcement.loadError')} />}
            <PostsContainer>
                <SearchBar
                    placeholder={t(showPinnedLinks ? 'searchPlaceholder' : 'announcement.searchPlaceholder')}
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
                        showPinnedLinks={showPinnedLinks}
                        onClose={handleClose}
                        onEdit={(urn, content) => updateListPostsCache(client, urn, content)}
                        onCreate={() => setTimeout(() => clearListPostsCache(client), REFRESH_AFTER_WRITE_DELAY_MS)}
                    />
                )}
            </PostsContainer>
        </PageContainer>
    );
};
