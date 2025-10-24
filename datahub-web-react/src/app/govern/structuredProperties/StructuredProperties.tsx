import { Button, PageTitle, SearchBar, Tooltip } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';

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
import { DISPLAY_NAME_FILTER_NAME } from '@app/search/utils/constants';
import { CREATED_TIME_FIELD_NAME } from '@app/searchV2/utils/constants';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useGetAutoCompleteResultsLazyQuery, useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { Entity, EntityType, SortOrder, StructuredPropertyEntity } from '@src/types.generated';

const MAX_PROPERTIES_TO_FETCH = 20;

const StructuredProperties = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [searchResults, setSearchResults] = useState<Entity[] | null>(null);
    const [newProperty, setNewProperty] = useState<StructuredPropertyEntity>();
    const [updatedProperty, setUpdatedProperty] = useState<StructuredPropertyEntity>();
    const [totalCount, setTotalCount] = useState<number>(0);

    const [isDrawerOpen, setIsDrawerOpen] = useState<boolean>(false);
    const [isViewDrawerOpen, setIsViewDrawerOpen] = useState<boolean>(false);
    const [selectedProperty, setSelectedProperty] = useState<StructuredPropertyEntity | undefined>();
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            types: [EntityType.StructuredProperty],
            query: searchQuery,
            start,
            count,
            searchFlags: { skipCache: true },
            sortInput: {
                sortCriteria: [{ field: CREATED_TIME_FIELD_NAME, sortOrder: SortOrder.Descending }],
            },
        }),
        [searchQuery],
    );

    const inputs = useMemo(() => getInputVariables(0, MAX_PROPERTIES_TO_FETCH), [getInputVariables]);

    const { data, loading, refetch } = useGetSearchResultsForMultipleQuery({
        variables: { input: inputs },
        skip: !!searchQuery,
        fetchPolicy: 'cache-first',
    });

    const [getAutoComplete, { data: autocompleteData }] = useGetAutoCompleteResultsLazyQuery();

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    useEffect(() => {
        if (!searchQuery) {
            setSearchResults(null);
            return;
        }

        getAutoComplete({
            variables: {
                input: {
                    query: searchQuery,
                    field: DISPLAY_NAME_FILTER_NAME,
                    limit: 200,
                    type: EntityType.StructuredProperty,
                },
            },
        });
    }, [searchQuery, getAutoComplete]);

    useEffect(() => {
        if (autocompleteData?.autoComplete?.entities) {
            setSearchResults(autocompleteData.autoComplete.entities);
        }
    }, [autocompleteData]);

    const searchAcrossEntities = data?.searchAcrossEntities;

    const badgeProperty = searchAcrossEntities?.searchResults?.find(
        (prop) => (prop.entity as StructuredPropertyEntity).settings?.showAsAssetBadge,
    )?.entity;

    useEffect(() => {
        if (searchAcrossEntities?.total !== undefined) {
            setTotalCount(searchAcrossEntities?.total);
        }
    }, [searchAcrossEntities?.total]);

    const fetchProperties = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            const result = await refetch?.({ input: getInputVariables(start, count) });
            return result?.data?.searchAcrossEntities?.searchResults.map((res) => res.entity) || [];
        },
        [refetch, getInputVariables],
    );

    const handleAddProperty = (property: StructuredPropertyEntity) => {
        setNewProperty(property);
        setTotalCount((prev) => prev + 1);
    };

    const handleUpdateProperty = (property: StructuredPropertyEntity) => {
        setUpdatedProperty(property);
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HeaderContainer>
                <HeaderContent>
                    <PageTitle
                        title="Structured Properties"
                        pillLabel={totalCount ? totalCount.toString() : undefined}
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
                    fetchData={fetchProperties}
                    totalCount={totalCount}
                    setTotalCount={setTotalCount}
                    pageSize={MAX_PROPERTIES_TO_FETCH}
                    searchResults={searchResults}
                    newProperty={newProperty}
                    updatedProperty={updatedProperty}
                />
            </TableContainer>
            <StructuredPropsDrawer
                isDrawerOpen={isDrawerOpen}
                setIsDrawerOpen={setIsDrawerOpen}
                refetch={refetch}
                selectedProperty={selectedProperty}
                setSelectedProperty={setSelectedProperty}
                badgeProperty={badgeProperty as StructuredPropertyEntity}
                handleAddProperty={handleAddProperty}
                handleUpdateProperty={handleUpdateProperty}
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
