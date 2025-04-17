import { SearchBar } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import FormsTable from './FormsTable';
import { useGetFormsData } from './useGetFormsData';

const Container = styled.div`
    display: flex;
    margin-top: 0;
    margin-left: 20px;
    margin-right: 20px;
    margin-bottom: 16px;
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

    const { inputs, searchData, loading, refetch, networkStatus } = useGetFormsData();

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <Container>
            <FormsSection>
                <SectionHeader />
                <SearchBar
                    placeholder="Search"
                    value={searchQuery}
                    onChange={(value) => handleSearch(value)}
                    style={{ maxWidth: '330px' }}
                />
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
