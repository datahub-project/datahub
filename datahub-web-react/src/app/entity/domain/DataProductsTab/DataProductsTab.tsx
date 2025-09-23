import { LoadingOutlined, PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Pagination } from 'antd';
import * as QueryString from 'query-string';
import React, { useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { DomainsPaginationContainer } from '@app/domain/DomainsList';
import CreateDataProductModal from '@app/entity/domain/DataProductsTab/CreateDataProductModal';
import DataProductResult from '@app/entity/domain/DataProductsTab/DataProductResult';
import { useEntityData } from '@app/entity/shared/EntityContext';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { SearchBar } from '@app/search/SearchBar';
import { DOMAINS_FILTER_NAME } from '@app/search/utils/constants';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataProduct, Domain, EntityType } from '@types';

const DataProductsPaginationWrapper = styled(DomainsPaginationContainer)`
    justify-content: center;
`;

const ResultsWrapper = styled.div`
    flex: 1;
    background-color: ${ANTD_GRAY[2]};
    padding: 16px;
    overflow: auto;
`;

const StyledLoading = styled(LoadingOutlined)`
    font-size: 32px;
`;

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 25%;
`;

const DEFAULT_PAGE_SIZE = 10;

export default function DataProductsTab() {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<string | undefined>(paramsQuery);
    const [page, setPage] = useState(1);
    const [isCreateModalVisible, setIsCreateModalVisible] = useState(false);
    const [createdDataProducts, setCreatedDataProducts] = useState<DataProduct[]>([]);
    const [editedDataProducts, setEditedDataProducts] = useState<DataProduct[]>([]);
    const [deletedDataProductUrns, setDeletedDataProductUrns] = useState<string[]>([]);

    const start = (page - 1) * DEFAULT_PAGE_SIZE;
    const domainUrn = entityData?.urn || '';

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        skip: !domainUrn,
        variables: {
            input: {
                types: [EntityType.DataProduct],
                query: query || '',
                start,
                count: DEFAULT_PAGE_SIZE,
                orFilters: [{ and: [{ field: DOMAINS_FILTER_NAME, values: [domainUrn] }] }],
                searchFlags: { skipCache: true },
            },
        },
        fetchPolicy: 'no-cache',
    });
    const totalResults = data?.searchAcrossEntities?.total || 0;
    const searchResults = data?.searchAcrossEntities?.searchResults?.map((r) => r.entity) || [];
    const dataProducts = [...createdDataProducts, ...searchResults];
    const displayedDataProducts = dataProducts
        .map(
            (dataProduct) =>
                editedDataProducts.find((editedDataProduct) => editedDataProduct.urn === dataProduct.urn) ||
                dataProduct,
        )
        .filter((dataProduct) => !deletedDataProductUrns.includes(dataProduct.urn));

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    function onCreateDataProduct(dataProduct: DataProduct) {
        setCreatedDataProducts([dataProduct, ...createdDataProducts]);
    }

    function onUpdateDataProduct(dataProduct: DataProduct) {
        setEditedDataProducts([dataProduct, ...editedDataProducts]);
    }

    return (
        <>
            <TabToolbar>
                <Button type="text" onClick={() => setIsCreateModalVisible(true)}>
                    <PlusOutlined /> New Data Product
                </Button>
                <SearchBar
                    initialQuery={query || ''}
                    placeholderText="Search data products..."
                    suggestions={[]}
                    style={{
                        maxWidth: 220,
                        padding: 0,
                    }}
                    inputStyle={{
                        height: 32,
                        fontSize: 12,
                    }}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q && q.length > 0 ? q : undefined)}
                    entityRegistry={entityRegistry}
                    hideRecommendations
                />
            </TabToolbar>
            <ResultsWrapper>
                {!loading && !displayedDataProducts.length && (
                    <Empty
                        description="No Data Products"
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        style={{ color: ANTD_GRAY[7] }}
                    />
                )}
                {loading && (
                    <LoadingWrapper>
                        <StyledLoading />
                    </LoadingWrapper>
                )}
                {!loading &&
                    displayedDataProducts.map((dataProduct) => (
                        <DataProductResult
                            key={dataProduct.urn}
                            dataProduct={dataProduct as DataProduct}
                            onUpdateDataProduct={onUpdateDataProduct}
                            setDeletedDataProductUrns={setDeletedDataProductUrns}
                        />
                    ))}
            </ResultsWrapper>
            <DataProductsPaginationWrapper>
                <Pagination
                    current={page}
                    pageSize={DEFAULT_PAGE_SIZE}
                    total={totalResults}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </DataProductsPaginationWrapper>
            {isCreateModalVisible && (
                <CreateDataProductModal
                    domain={entityData as Domain}
                    onCreateDataProduct={onCreateDataProduct}
                    onClose={() => setIsCreateModalVisible(false)}
                />
            )}
        </>
    );
}
