import { Button, SearchBar, Text } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Pagination, Radio, Table, Typography, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import ColumnViewBuilderModal, { columnViewToBuilderState } from '@app/entityV2/columnView/builder/ColumnViewBuilderModal';
import ColumnViewDropdownMenu from '@app/entityV2/columnView/menu/ColumnViewDropdownMenu';
import { DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, SCHEMA_TARGET } from '@app/entityV2/columnView/types';
import { EmptyContainer } from '@app/entityV2/view/ViewsList';
import { GlobalDefaultViewIcon } from '@app/entityV2/view/shared/GlobalDefaultViewIcon';
import { UserDefaultViewIcon } from '@app/entityV2/view/shared/UserDefaultViewIcon';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';

import {
    useGetGlobalColumnViewsSettingsQuery,
    useListGlobalColumnViewsQuery,
    useListMyColumnViewsQuery,
} from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubViewType } from '@types';

const Container = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding-top: 7px;
`;

const Toolbar = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    padding: 1px 0 16px 0; // 1px at the top to prevent Select's border outline from cutting-off
    flex-shrink: 0;
`;

const ToolbarGroup = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 300px;
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

const NameCell = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
`;

/**
 * Settings > Views > Columns. Parameterized copy of view/ManageViews + ViewsList: a personal/public
 * toggle, server-side search + pagination over listMyColumnViews / listGlobalColumnViews, default
 * badges (personal / organization), and create / edit through ColumnViewBuilderModal.
 *
 * Routing: SettingsPage mounts ManageViews on the non-exact `${path}/views` route, so
 * `/settings/views/columns` already renders here through ManageViews' Columns tab.
 */
export default function ManageColumnViews() {
    const { t } = useTranslation('entity.views');
    const theme = useTheme();
    const userContext = useUserContext();

    const [viewType, setViewType] = useState<DataHubViewType>(DataHubViewType.Personal);
    const [page, setPage] = useState(1);
    const [query, setQuery] = useState<string | undefined>(undefined);
    const [builder, setBuilder] = useState<{ open: boolean; view?: DataHubColumnView }>({ open: false });

    const pageSize = DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE;
    const start = (page - 1) * pageSize;
    const isPersonal = viewType === DataHubViewType.Personal;

    const {
        loading: loadingPersonal,
        error: errorPersonal,
        data: dataPersonal,
        refetch: refetchPersonal,
    } = useListMyColumnViewsQuery({
        variables: { start, count: pageSize, query: query || undefined, viewType: DataHubViewType.Personal, target: SCHEMA_TARGET },
        fetchPolicy: 'cache-first',
        skip: !isPersonal,
    });
    const {
        loading: loadingGlobal,
        error: errorGlobal,
        data: dataGlobal,
        refetch: refetchGlobal,
    } = useListGlobalColumnViewsQuery({
        variables: { start, count: pageSize, query: query || undefined, target: SCHEMA_TARGET },
        fetchPolicy: 'cache-first',
        skip: isPersonal,
    });

    // Defaults: personal from the already-loaded user settings, organization from global settings.
    const personalDefaultUrn = (userContext.user?.settings?.columnViews?.defaults || []).find(
        (d) => d.target === SCHEMA_TARGET,
    )?.view?.urn;
    const { data: globalSettings } = useGetGlobalColumnViewsSettingsQuery({ fetchPolicy: 'cache-first' });
    const orgDefaultUrn = globalSettings?.globalColumnViewsSettings?.defaults?.find((d) => d.target === SCHEMA_TARGET)?.view;

    const listData = isPersonal ? dataPersonal?.listMyColumnViews : dataGlobal?.listGlobalColumnViews;
    const loading = loadingPersonal || loadingGlobal;
    const error = errorPersonal || errorGlobal;
    const total = listData?.total || 0;
    const views = (listData?.columnViews || []) as DataHubColumnView[];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };
    const onChangeQuery = (q: string) => {
        setPage(1);
        setQuery(q || undefined);
    };
    const closeBuilder = () => setBuilder({ open: false });
    const onBuilderSubmit = () => {
        closeBuilder();
        (isPersonal ? refetchPersonal : refetchGlobal)();
    };

    const columns = [
        {
            title: t('table.name'),
            dataIndex: 'name',
            key: 'name',
            render: (name: string, v: DataHubColumnView) => (
                <NameCell>
                    <Typography.Link onClick={() => setBuilder({ open: true, view: v })}>{name}</Typography.Link>
                    {v.urn === personalDefaultUrn && <UserDefaultViewIcon title={t('columnViews.myDefaultTooltip')} />}
                    {v.urn === orgDefaultUrn && <GlobalDefaultViewIcon title={t('columnViews.orgDefaultTooltip')} />}
                </NameCell>
            ),
        },
        { title: t('table.description'), dataIndex: 'description', key: 'description' },
        { title: t('table.type'), dataIndex: 'viewType', key: 'viewType' },
        {
            title: t('columnViews.columnsCount'),
            key: 'columns',
            width: 120,
            render: (_: unknown, v: DataHubColumnView) => v.definition.columns.length,
        },
        {
            title: '',
            key: 'actions',
            width: 48,
            render: (_: unknown, v: DataHubColumnView) => (
                <ColumnViewDropdownMenu view={v} onEdit={() => setBuilder({ open: true, view: v })} />
            ),
        },
    ];

    return (
        <Container>
            {!listData && loading && <Message type="loading" content={t('loading')} />}
            {error && message.error({ content: t('loadError'), duration: 3 })}
            <Toolbar>
                <ToolbarGroup>
                    <Radio.Group
                        optionType="button"
                        value={viewType}
                        onChange={(e) => {
                            setPage(1);
                            setViewType(e.target.value);
                        }}
                        options={[
                            { value: DataHubViewType.Personal, label: t('personalTab') },
                            { value: DataHubViewType.Global, label: t('publicTab') },
                        ]}
                    />
                    <StyledSearchBar placeholder={t('columnViews.searchPlaceholder')} onChange={onChangeQuery} value={query || ''} />
                </ToolbarGroup>
                <Button
                    variant="filled"
                    id="create-new-column-view-button"
                    data-testid="create-new-column-view-button"
                    icon={{ icon: Plus }}
                    onClick={() => setBuilder({ open: true })}
                >
                    {t('columnViews.create')}
                </Button>
            </Toolbar>
            {!total && !loading ? (
                <EmptyContainer>
                    <Text size="md" weight="bold" style={{ color: theme.colors.textSecondary }}>
                        {t('emptyTitle')}
                    </Text>
                </EmptyContainer>
            ) : (
                <>
                    <TableContainer>
                        <Table rowKey="urn" dataSource={views} columns={columns} pagination={false} />
                    </TableContainer>
                    {total >= pageSize && (
                        <PaginationContainer>
                            <StyledPagination
                                current={page}
                                pageSize={pageSize}
                                total={total}
                                showLessItems
                                onChange={onChangePage}
                                showSizeChanger={false}
                            />
                        </PaginationContainer>
                    )}
                </>
            )}
            {builder.open && (
                <ColumnViewBuilderModal
                    urn={builder.view?.urn}
                    initialState={builder.view ? columnViewToBuilderState(builder.view) : undefined}
                    onSubmit={onBuilderSubmit}
                    onCancel={closeBuilder}
                />
            )}
        </Container>
    );
}
