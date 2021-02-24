import { FilterOutlined } from '@ant-design/icons';
import { Alert, Button, Card, Divider, List, Pagination, Typography } from 'antd';
import * as React from 'react';
import { SearchCfg } from '../../conf';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { EntityType, FacetFilterInput } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { Message } from '../shared/Message';
import { useEntityRegistry } from '../useEntityRegistry';

interface Props {
    type: EntityType;
    query: string;
    page: number;
    filters: Array<FacetFilterInput>;
    onAddFilters: () => void;
    onChangePage: (page: number) => void;
}

export const EntitySearchResults = ({ type, query, page, filters, onAddFilters, onChangePage }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data } = useGetSearchResultsQuery({
        variables: {
            input: {
                type,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters,
            },
        },
    });

    const results = data?.search?.entities || [];
    const pageStart = data?.search?.start || 0;
    const pageSize = data?.search?.count || 0;
    const totalResults = data?.search?.total || 0;
    const lastResultIndex =
        pageStart * pageSize + pageSize > totalResults ? totalResults : pageStart * pageSize + pageSize;

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <Button style={{ backgroundColor: '#F5F5F5' }} color="#F5F5F5" onClick={onAddFilters}>
                <FilterOutlined />
                Add Filters
            </Button>
            <Typography.Paragraph style={{ color: 'gray', marginTop: '36px' }}>
                Showing{' '}
                <b>
                    {(page - 1) * pageSize} - {lastResultIndex}
                </b>{' '}
                of <b>{totalResults}</b> results
            </Typography.Paragraph>
            <List
                header={
                    <Card
                        bodyStyle={{ padding: '16px 24px' }}
                        style={{ right: '52px', top: '-40px', position: 'absolute' }}
                    >
                        {entityRegistry.getIcon(type, 36, IconStyleType.ACCENT)}
                    </Card>
                }
                style={{ width: '100%', borderColor: '#f0f0f0', marginTop: '12px', padding: '16px 32px' }}
                dataSource={results}
                split={false}
                renderItem={(item, index) => (
                    <>
                        <List.Item>{entityRegistry.renderSearchResult(type, item)}</List.Item>
                        {index < results.length - 1 && <Divider />}
                    </>
                )}
                bordered
            />
            <Pagination
                style={{ width: '100%', display: 'flex', justifyContent: 'center', margin: '40px 0px' }}
                current={page}
                pageSize={pageSize}
                total={totalResults}
                showLessItems
                onChange={onChangePage}
            />
        </>
    );
};
