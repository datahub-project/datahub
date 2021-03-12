import * as React from 'react';
import { List } from 'antd';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../shared/Message';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';

interface Props {
    query: string;
}

const RESULTS_PER_GROUP = 3;

export const AllEntitiesSearchResults = ({ query }: Props) => {
    const allSearchResultsByType = useGetAllEntitySearchResults({
        query,
        start: 0,
        count: RESULTS_PER_GROUP,
        filters: null,
    });

    const loading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    const noResults = Object.keys(allSearchResultsByType).every((type) => {
        return (
            !allSearchResultsByType[type].loading && allSearchResultsByType[type].data?.search?.entities.length === 0
        );
    });

    const noResultsView = <List style={{ margin: '28px 160px' }} bordered dataSource={[]} />;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {noResults && noResultsView}
            {Object.keys(allSearchResultsByType).map((type: any) => {
                const entities = allSearchResultsByType[type].data?.search?.entities;
                if (entities && entities.length > 0) {
                    return <EntityGroupSearchResults type={type} query={query} entities={entities} />;
                }
                return null;
            })}
        </>
    );
};
