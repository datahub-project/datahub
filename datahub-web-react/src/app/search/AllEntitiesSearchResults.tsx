import React from 'react';
import { List } from 'antd';
import { GetAllEntitySearchResultsType } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../shared/Message';
import { EntityGroupSearchResults } from './EntityGroupSearchResults';

interface Props {
    query: string;
    allSearchResultsByType: GetAllEntitySearchResultsType;
    loading: boolean;
    noResults: boolean;
}

export const AllEntitiesSearchResults = ({ query, allSearchResultsByType, loading, noResults }: Props) => {
    const noResultsView = (
        <List
            style={{ margin: '28px 160px', boxShadow: '0px 0px 30px 0px rgb(234 234 234)' }}
            bordered
            dataSource={[]}
        />
    );

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
