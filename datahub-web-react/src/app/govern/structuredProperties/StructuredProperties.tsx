import { Button, PageTitle, SearchBar, Tooltip } from '@components';
import React, { useState } from 'react';

import StructuredPropsDrawer from '@app/govern/structuredProperties/StructuredPropsDrawer';
import StructuredPropsTable from '@app/govern/structuredProperties/StructuredPropsTable';
import ViewStructuredPropsDrawer from '@app/govern/structuredProperties/ViewStructuredPropsDrawer';
import {
    ButtonContainer,
    HeaderContainer,
    HeaderContent,
    PageContainer,
    TableContainer,
} from '@app/govern/structuredProperties/styledComponents';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SearchResult, StructuredPropertyEntity } from '@src/types.generated';

const StructuredProperties = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
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
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
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
                            icon={{ icon: 'Add', source: 'material' }}
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
            <SearchBar
                placeholder="Search"
                value={searchQuery}
                onChange={(value) => handleSearch(value)}
                width="272px"
            />
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
