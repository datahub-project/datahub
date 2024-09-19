import { Button, Text } from '@src/alchemy-components';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SearchResult } from '@src/types.generated';
import React, { useState } from 'react';
import StructuredPropsDrawer from './StructuredPropsDrawer';
import StructuredPropsTable from './StructuredPropsTable';
import {
    ButtonContainer,
    HeaderContainer,
    HeaderContent,
    PageContainer,
    StyledSearch,
    TableContainer,
} from './styledComponents';

const StructuredProperties = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [isDrawerOpen, setIsDrawerOpen] = useState<boolean>(false);
    const [selectedProperty, setSelectedProperty] = useState<SearchResult | undefined>();

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 500,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const { data, loading, refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    const searchAcrossEntities = data?.searchAcrossEntities;

    return (
        <PageContainer>
            <HeaderContainer>
                <HeaderContent>
                    <Text size="xl" weight="bold">
                        Structured Properties
                    </Text>
                    <Text color="gray" weight="medium">
                        Information about this page
                    </Text>
                </HeaderContent>
                <ButtonContainer>
                    <Button icon="Add" onClick={() => setIsDrawerOpen(true)}>
                        Create
                    </Button>
                </ButtonContainer>
            </HeaderContainer>
            <StyledSearch
                placeholder="Search"
                onSearch={handleSearch}
                onChange={(e) => handleSearch(e.target.value)}
                allowClear
            />

            <TableContainer>
                <StructuredPropsTable
                    searchQuery={searchQuery}
                    data={data}
                    loading={loading}
                    setIsDrawerOpen={setIsDrawerOpen}
                    setSelectedProperty={setSelectedProperty}
                    selectedProperty={selectedProperty}
                    inputs={inputs}
                    searchAcrossEntities={searchAcrossEntities}
                />
            </TableContainer>
            <StructuredPropsDrawer
                isDrawerOpen={isDrawerOpen}
                setIsDrawerOpen={setIsDrawerOpen}
                refetch={refetch}
                selectedProperty={selectedProperty}
                setSelectedProperty={setSelectedProperty}
                inputs={inputs}
                searchAcrossEntities={searchAcrossEntities}
            />
        </PageContainer>
    );
};

export default StructuredProperties;
