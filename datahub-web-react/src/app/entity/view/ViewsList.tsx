import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import { Button, message, Modal, Pagination } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useApolloClient } from '@apollo/client';
import * as QueryString from 'query-string';
import {
    useCreateViewMutation,
    useDeleteViewMutation,
    useListMyViewsQuery,
    useUpdateViewMutation,
} from '../../../graphql/view.generated';
import { DataHubView } from '../../../types.generated';
import { SearchBar } from '../../search/SearchBar';
import TabToolbar from '../shared/components/styled/TabToolbar';
import { Message } from '../../shared/Message';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ViewBuilderState } from './types';
import { ViewBuilderModal } from './builder/ViewBuilderModal';
import { useUserContext } from '../../context/useUserContext';
import { scrollToTop } from '../../shared/searchUtils';
import { ViewsTable } from './ViewsTable';
import { convertStateToUpdateInput, updateListViewsCache, removeFromListViewsCache } from './utils';

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

const searchBarStyle = {
    maxWidth: 220,
    padding: 0,
};

const searchBarInputStyle = {
    height: 32,
    fontSize: 12,
};

/**
 * Number of Views on each page.
 */
const DEFAULT_PAGE_SIZE = 25;

/**
 * This component renders a paginated list of Views.
 */
export const ViewsList = () => {
    /**
     * Context
     */
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();

    /**
     * Query Params
     */
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;

    /**
     * State
     */
    const [page, setPage] = useState(1);
    const [showViewBuilder, setShowViewBuilder] = useState<boolean>(false);
    const [selectedViewUrn, setSelectedViewUrn] = useState<undefined | string>(undefined);
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    /**
     * Queries
     */
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;
    const client = useApolloClient();
    const { loading, error, data, refetch } = useListMyViewsQuery({
        variables: {
            start,
            count: pageSize,
            query,
        },
        fetchPolicy: 'cache-first',
    });
    const [deleteViewMutation] = useDeleteViewMutation();
    const [updateViewMutation] = useUpdateViewMutation();
    const [createViewMutation] = useCreateViewMutation();

    /**
     * Upserts a new view.
     *
     * @param state the state, which defines the fields of the new View.
     */
    const upsertView = (state: ViewBuilderState) => {
        const viewInput = convertStateToUpdateInput(state);
        const isCreate = selectedViewUrn === undefined;
        const mutation = isCreate ? createViewMutation : updateViewMutation;
        const variables = selectedViewUrn
            ? { urn: selectedViewUrn, input: { ...viewInput, viewType: undefined } }
            : {
                  input: viewInput,
              };
        (
            mutation({
                variables: variables as any,
            }) as any
        )
            .then((res) => {
                message.success({
                    content: `${selectedViewUrn ? 'Edited' : 'Created'} View!`,
                    duration: 3,
                });
                const updatedUrn = selectedViewUrn || res.data?.createView || '';
                updateListViewsCache(updatedUrn, state, client, page, pageSize, !isCreate, query);
                setShowViewBuilder(false);
                setSelectedViewUrn(undefined);

                setTimeout(() => {
                    refetch?.();
                }, 2000);

                /**
                 * Set the newly View as the user's session default.
                 */
                userContext.updateState({
                    ...userContext.state,
                    views: {
                        selectedView: {
                            ...(viewInput as DataHubView),
                            urn: res.data?.createView,
                        },
                    },
                });
            })
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to save View! An unexpected error occurred.`,
                    duration: 3,
                });
            });
    };

    /**
     * Deletes an existing View. Then adds the urn to a list
     * of removed urns, which are used when displaying the new View list.
     *
     * @param urn the urn of the View.
     */
    const deleteView = async (urn: string) => {
        deleteViewMutation({
            variables: { urn },
        })
            .then(() => {
                removeFromListViewsCache(urn, client, page, pageSize, query);

                /**
                 * Clear the selected view urn from local state,
                 * if the deleted view was that urn.
                 */
                if (urn === userContext.localState?.selectedViewUrn) {
                    userContext.updateLocalState({
                        ...userContext.localState,
                        selectedViewUrn: undefined,
                    });
                }
                message.success({ content: 'Removed View.', duration: 2 });
            })
            .catch((_) => {
                message.destroy();
                message.error({ content: `Failed to remove View. An unexpected error occurred.`, duration: 3 });
            });
    };

    /**
     * Prompts the user to confirm, then deletes an existing View.
     *
     * @param urn the urn of the View.
     */
    const onDeleteView = (urn: string) => {
        Modal.confirm({
            title: `Confirm View Removal`,
            content: `Are you sure you want to remove this View?`,
            onOk() {
                deleteView(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onClickCreateView = () => {
        setShowViewBuilder(true);
    };

    const onClickEditView = (urn: string) => {
        setShowViewBuilder(true);
        setSelectedViewUrn(urn);
    };

    const onCancel = () => {
        setShowViewBuilder(false);
        setSelectedViewUrn(undefined);
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    /**
     * Variables
     */
    const totalViews = data?.listMyViews?.total || 0;
    const views = data?.listMyViews?.views || [];
    const selectedView = (selectedViewUrn && views.find((view) => view.urn === selectedViewUrn)) || undefined;

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading Views..." />}
            {error && message.error({ content: `Failed to load Views! An unexpected error occurred.`, duration: 3 })}
            <TabToolbar>
                <Button type="text" onClick={onClickCreateView}>
                    <PlusOutlined /> Create new View
                </Button>
                <SearchBar
                    initialQuery=""
                    placeholderText="Search Views..."
                    suggestions={[]}
                    style={searchBarStyle}
                    inputStyle={searchBarInputStyle}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q)}
                    entityRegistry={entityRegistry}
                />
            </TabToolbar>
            <ViewsTable views={views} onEditView={onClickEditView} onDeleteView={onDeleteView} />
            <PaginationContainer>
                <StyledPagination
                    current={page}
                    pageSize={pageSize}
                    total={totalViews}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </PaginationContainer>
            {showViewBuilder && (
                <ViewBuilderModal initialState={selectedView} onSubmit={upsertView} onCancel={onCancel} />
            )}
        </>
    );
};
