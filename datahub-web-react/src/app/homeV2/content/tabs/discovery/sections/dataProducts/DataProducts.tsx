import { Skeleton } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType, HomePageModule } from '@app/analytics';
import { HorizontalListSkeletons } from '@app/homeV2/content/HorizontalListSkeletons';
import { Section } from '@app/homeV2/content/tabs/discovery/sections/Section';
import { DataProductCard } from '@app/homeV2/content/tabs/discovery/sections/dataProducts/DataProductCard';
import { useGetDataProducts } from '@app/homeV2/content/tabs/discovery/sections/dataProducts/useGetDataProducts';
import { HOME_PAGE_DATA_PRODUCTS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { Carousel } from '@app/sharedV2/carousel/Carousel';
import { PageRoutes } from '@conf/Global';

const SkeletonCard = styled(Skeleton.Button)<{ width: string }>`
    &&& {
        height: 83px;
        width: 287px;
    }
`;

export const DataProducts = () => {
    const history = useHistory();
    const { dataProducts, loading } = useGetDataProducts();

    const navigateToDataProducts = () => {
        analytics.event({
            type: EventType.HomePageClick,
            module: HomePageModule.Discover,
            section: 'Data Products',
            value: 'View all',
        });
        history.push(PageRoutes.DATA_PRODUCTS);
    };

    const handleDataProductClick = (dataProductUrn: string) => {
        analytics.event({
            type: EventType.HomePageClick,
            module: HomePageModule.Discover,
            section: 'Data Products',
            value: dataProductUrn,
        });
    };

    return (
        <div id={HOME_PAGE_DATA_PRODUCTS_ID}>
            {loading && <HorizontalListSkeletons Component={SkeletonCard} />}
            {!loading && dataProducts && !!dataProducts.length && (
                <Section title="Data Products" actionText="View all" onClickAction={navigateToDataProducts}>
                    <Carousel>
                        {dataProducts.map((item) => {
                            const { dataProduct, domain } = item;
                            return (
                                // eslint-disable-next-line
                                <span key={dataProduct.urn} onClick={() => handleDataProductClick(dataProduct.urn)}>
                                    <DataProductCard dataProduct={dataProduct} domain={domain} />
                                </span>
                            );
                        })}
                    </Carousel>
                </Section>
            )}
        </div>
    );
};
