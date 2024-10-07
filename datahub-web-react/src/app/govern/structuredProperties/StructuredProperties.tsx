import { useUserContext } from '@src/app/context/useUserContext';
import { Tooltip } from 'antd';
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
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;

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
                        Create and manage structured properties for your organization&apos;s data assets
                    </Text>
                </HeaderContent>
                <Tooltip
                    showArrow={false}
                    title={
                        !canEditProps
                            ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                            : null
                    }
                >
                    <ButtonContainer>
                        <Button disabled={!canEditProps} icon="Add" onClick={() => setIsDrawerOpen(true)}>
                            Create
                        </Button>
                    </ButtonContainer>
                </Tooltip>
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
