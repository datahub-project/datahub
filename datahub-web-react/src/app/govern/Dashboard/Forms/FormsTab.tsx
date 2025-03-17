import { SearchBar } from '@components';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import React, { useState } from 'react';
import styled from 'styled-components';
import FormsTable from './FormsTable';

const Container = styled.div`
    display: flex;
    margin-top: 0;
    margin-left: 20px;
    margin-right: 20px;
    margin-bottom: 16px;
    overflow: auto;
    height: calc(100% - 16px);
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;

const FormsSection = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const FormsContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
    margin-top: 16px;
`;

const FormsTab = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');

    const inputs = {
        types: [EntityType.Form],
        query: '*',
        start: 0,
        count: 200,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const {
        data: searchData,
        loading,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <Container>
            <FormsSection>
                <SectionHeader />
                <SearchBar placeholder="Search" value={searchQuery} onChange={(value) => handleSearch(value)} />
                <FormsContainer>
                    <FormsTable
                        searchQuery={searchQuery}
                        searchData={searchData}
                        loading={loading}
                        networkStatus={networkStatus}
                        refetch={refetch}
                        inputs={inputs}
                    />
                </FormsContainer>
            </FormsSection>
        </Container>
    );
};

export default FormsTab;
