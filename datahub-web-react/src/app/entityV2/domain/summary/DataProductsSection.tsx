import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import { useEntityData } from '../../../entity/shared/EntityContext';
import ContentSectionLoading from './ContentSectionLoading';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { DataProduct, EntityType } from '../../../../types.generated';
import { DOMAINS_FILTER_NAME } from '../../../searchV2/utils/constants';
import { DataProductMiniPreview } from '../../shared/links/DataProductMiniPreview';
import {
    SectionContainer,
    SummaryTabHeaderTitle,
    SummaryTabHeaderWrapper,
    SummaryHeaderButtonGroup,
} from '../../shared/summary/HeaderComponents';
import { navigateToDomainDataProducts } from '../../shared/containers/profile/sidebar/Domain/utils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../../entity/Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { Carousel } from '../../../sharedV2/carousel/Carousel';
import SectionActionButton from '../../shared/containers/profile/sidebar/SectionActionButton';

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
