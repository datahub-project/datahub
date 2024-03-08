import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { useEntityData } from '../../shared/EntityContext';
import ContentSectionLoading from './ContentSectionLoading';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { DataProduct, EntityType } from '../../../../types.generated';
import { DOMAINS_FILTER_NAME } from '../../../searchV2/utils/constants';
import { DataProductMiniPreview } from '../../shared/links/DataProductMiniPreview';
import { Container, SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '../../shared/summary/HeaderComponents';
import { navigateToDomainDataProducts } from '../../shared/containers/profile/sidebar/Domain/utils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { DataProductMiniPreviewAddDataProduct } from '../../shared/links/DataProductMiniPreviewAddDataProduct';
import { IconStyleType } from '../../../entity/Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { Carousel } from '../../../shared/carousel/Carousel';

const ViewAllButton = styled.div`
    color: ${ANTD_GRAY[7]};
    padding: 2px;
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

export const DataProductsRow = () => {
    const { urn, entityType, entityData } = useEntityData();
    const history = useHistory();
    const domainUrn = entityData?.urn || '';
    const entityRegistry = useEntityRegistry();

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        skip: !domainUrn,
        variables: {
            input: {
                types: [EntityType.DataProduct],
                query: '',
                start: 0,
                count: 5,
                orFilters: [{ and: [{ field: DOMAINS_FILTER_NAME, values: [domainUrn] }] }],
                searchFlags: { skipCache: true },
            },
        },
    });

    const dataProducts = data?.searchAcrossEntities?.searchResults.map((r) => r.entity) || [];
    const showAddDataProductLink = dataProducts.length <= 2;

    return (
        <Container>
            <SummaryTabHeaderWrapper>
                <SummaryTabHeaderTitle
                    icon={entityRegistry.getIcon(EntityType.DataProduct, 16, IconStyleType.ACCENT, ANTD_GRAY[8])}
                    title={`Data Products (${dataProducts.length})`}
                />
                <ViewAllButton onClick={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry)}>
                    view all
                </ViewAllButton>
            </SummaryTabHeaderWrapper>
            {loading && <ContentSectionLoading />}
            <Carousel>
                {!loading &&
                    dataProducts.map((product) => (
                        <DataProductMiniPreview dataProduct={product as DataProduct} key={product.urn} />
                    ))}
                {showAddDataProductLink && (
                    <DataProductMiniPreviewAddDataProduct
                        onAdd={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry, true)}
                    />
                )}
            </Carousel>
        </Container>
    );
};
