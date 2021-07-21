import React, { useEffect } from 'react';
import { List } from 'antd';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../shared/Message';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';
import analytics, { EventType } from '../analytics';

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
            !allSearchResultsByType[type].loading &&
            allSearchResultsByType[type].data?.search?.searchResults.length === 0
        );
    });

    const noResultsView = (
        <List
            style={{ margin: '28px 160px', boxShadow: '0px 0px 30px 0px rgb(234 234 234)' }}
            bordered
            dataSource={[]}
        />
    );

    useEffect(() => {
        if (!loading) {
            let resultCount = 0;
            Object.keys(allSearchResultsByType).forEach((key) => {
                if (allSearchResultsByType[key].loading) {
                    resultCount += 0;
                } else {
                    resultCount += allSearchResultsByType[key].data?.search?.total;
                }
            });

            analytics.event({
                type: EventType.SearchResultsViewEvent,
                query,
                total: resultCount,
            });
        }
    }, [query, allSearchResultsByType, loading]);

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {noResults && noResultsView}
            {Object.keys(allSearchResultsByType).map((type: any) => {
                const searchResults = allSearchResultsByType[type].data?.search?.searchResults;
                if (searchResults && searchResults.length > 0) {
                    return (
                        <EntityGroupSearchResults key={type} type={type} query={query} searchResults={searchResults} />
                    );
                }
                return null;
            })}
        </>
    );
};
