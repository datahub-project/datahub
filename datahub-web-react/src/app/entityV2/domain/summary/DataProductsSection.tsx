import AddRoundedIcon from '@mui/icons-material/AddRounded';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { useEntityData } from '@app/entity/shared/EntityContext';
import ContentSectionLoading from '@app/entityV2/domain/summary/ContentSectionLoading';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { navigateToDomainDataProducts } from '@app/entityV2/shared/containers/profile/sidebar/Domain/utils';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { DataProductMiniPreview } from '@app/entityV2/shared/links/DataProductMiniPreview';
import {
    SectionContainer,
    SummaryHeaderButtonGroup,
    SummaryTabHeaderTitle,
    SummaryTabHeaderWrapper,
} from '@app/entityV2/shared/summary/HeaderComponents';
import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { Carousel } from '@app/sharedV2/carousel/Carousel';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataProduct, EntityType } from '@types';

const ViewAllButton = styled.div`
    color: ${ANTD_GRAY[7]};
    padding: 2px;

    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

const StyledCarousel = styled(Carousel)`
    align-items: stretch;
`;

export const DataProductsSection = () => {
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

    const dataProducts = data?.searchAcrossEntities?.searchResults?.map((r) => r.entity) || [];
    const numDataProducts = data?.searchAcrossEntities?.total || 0;

    if (!numDataProducts) {
        return null;
    }

    return (
        <SectionContainer>
            <SummaryTabHeaderWrapper>
                <SummaryHeaderButtonGroup>
                    <SummaryTabHeaderTitle
                        icon={entityRegistry.getIcon(EntityType.DataProduct, 16, IconStyleType.ACCENT, ANTD_GRAY[8])}
                        title={`Data Products (${numDataProducts})`}
                    />
                    <SectionActionButton
                        tip="Create Data Product"
                        button={<AddRoundedIcon />}
                        onClick={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry, true)}
                    />
                </SummaryHeaderButtonGroup>
                <ViewAllButton onClick={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry)}>
                    View all
                </ViewAllButton>
            </SummaryTabHeaderWrapper>
            {loading && <ContentSectionLoading />}
            <StyledCarousel>
                {!loading &&
                    dataProducts.map((product) => (
                        <DataProductMiniPreview dataProduct={product as DataProduct} key={product.urn} />
                    ))}
            </StyledCarousel>
        </SectionContainer>
    );
};
