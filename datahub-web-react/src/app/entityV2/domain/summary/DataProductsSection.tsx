import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';
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
import { ANTD_GRAY, REDESIGN_COLORS } from '../../shared/constants';
import { Carousel } from '../../../sharedV2/carousel/Carousel';

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

// Styled to match the Summary page tabs
const StyledAddButton = styled(Button)`
    background-color: transparent;
    color: ${REDESIGN_COLORS.PRIMARY_PURPLE};
    border-color: ${REDESIGN_COLORS.PRIMARY_PURPLE};

    :hover {
        background-color: transparent;
        color: ${REDESIGN_COLORS.HOVER_PURPLE};
        border-color: ${REDESIGN_COLORS.HOVER_PURPLE};
    }
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

    const dataProducts = data?.searchAcrossEntities?.searchResults.map((r) => r.entity) || [];
    const numDataProducts = data?.searchAcrossEntities?.total || 0;

    return (
        <SectionContainer>
            <SummaryTabHeaderWrapper>
                <SummaryHeaderButtonGroup>
                    <SummaryTabHeaderTitle
                        icon={entityRegistry.getIcon(EntityType.DataProduct, 16, IconStyleType.ACCENT, ANTD_GRAY[8])}
                        title={`Data Products (${numDataProducts})`}
                    />
                    <StyledAddButton
                        type="primary"
                        size="small"
                        icon={<PlusOutlined />}
                        onClick={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry, true)}
                    >
                        Add Data Product
                    </StyledAddButton>
                </SummaryHeaderButtonGroup>
                <ViewAllButton onClick={() => navigateToDomainDataProducts(urn, entityType, history, entityRegistry)}>
                    view all
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
