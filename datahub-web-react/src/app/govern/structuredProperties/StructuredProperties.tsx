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
    const [currentProperty, setCurrentProperty] = useState<SearchResult | undefined>();

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
    });

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
                    setCurrentProperty={setCurrentProperty}
                    currentProperty={currentProperty}
                    refetch={refetch}
                />
            </TableContainer>
            <StructuredPropsDrawer
                isDrawerOpen={isDrawerOpen}
                setIsDrawerOpen={setIsDrawerOpen}
                refetch={refetch}
                currentProperty={currentProperty}
                setCurrentProperty={setCurrentProperty}
            />
        </PageContainer>
    );
};

export default StructuredProperties;
