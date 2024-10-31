import React, { useState } from 'react';
import { SearchOutlined } from '@ant-design/icons';
import { Button } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SearchResult } from '@src/types.generated';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { Tooltip } from 'antd';
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
    const noOfProperties = searchAcrossEntities?.searchResults?.length;

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
            <StyledSearch
                placeholder="Search"
                onChange={(e) => handleSearch(e.target.value)}
                allowClear
                prefix={<SearchOutlined />}
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
