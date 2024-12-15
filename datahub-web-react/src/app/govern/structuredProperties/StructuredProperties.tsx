import { Button, PageTitle, SearchBar, Tooltip } from '@components';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import React, { useState } from 'react';
import StructuredPropsDrawer from './StructuredPropsDrawer';
import StructuredPropsTable from './StructuredPropsTable';
import { ButtonContainer, HeaderContainer, HeaderContent, PageContainer, TableContainer } from './styledComponents';
import ViewStructuredPropsDrawer from './ViewStructuredPropsDrawer';

const StructuredProperties = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [isDrawerOpen, setIsDrawerOpen] = useState<boolean>(false);
    const [isViewDrawerOpen, setIsViewDrawerOpen] = useState<boolean>(false);
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
    const noOfProperties = searchAcrossEntities?.searchResults?.length;
    const badgeProperty = searchAcrossEntities?.searchResults?.find(
        (prop) => (prop.entity as StructuredPropertyEntity).settings?.showAsAssetBadge,
    )?.entity;

    return (
        <PageContainer>
            <HeaderContainer>
                <HeaderContent>
                    <PageTitle
                        title="Structured Properties"
                        pillLabel={noOfProperties ? noOfProperties.toString() : undefined}
                        subTitle="Create and manage custom properties for your data assets"
                    />
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
                        <Button
                            disabled={!canEditProps}
                            icon="Add"
                            data-testid="structured-props-create-button"
                            onClick={() => {
                                setIsDrawerOpen(true);
                                analytics.event({ type: EventType.CreateStructuredPropertyClickEvent });
                            }}
                        >
                            Create
                        </Button>
                    </ButtonContainer>
                </Tooltip>
            </HeaderContainer>
            <SearchBar placeholder="Search" value={searchQuery} onChange={(value) => handleSearch(value)} />
            <TableContainer>
                <StructuredPropsTable
                    searchQuery={searchQuery}
                    data={data}
                    loading={loading}
                    setIsDrawerOpen={setIsDrawerOpen}
                    setIsViewDrawerOpen={setIsViewDrawerOpen}
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
                badgeProperty={badgeProperty as StructuredPropertyEntity}
            />
            {selectedProperty && (
                <ViewStructuredPropsDrawer
                    isViewDrawerOpen={isViewDrawerOpen}
                    setIsViewDrawerOpen={setIsViewDrawerOpen}
                    selectedProperty={selectedProperty}
                    setSelectedProperty={setSelectedProperty}
                />
            )}
        </PageContainer>
    );
};

export default StructuredProperties;
