import { Skeleton } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Section } from '../Section';
import { DataProductCard } from './DataProductCard';
import { useGetDataProducts } from './useGetDataProducts';
import { PageRoutes } from '../../../../../../../conf/Global';
import { HOME_PAGE_DATA_PRODUCTS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import { Carousel } from '../../../../../../sharedV2/carousel/Carousel';
import { HorizontalListSkeletons } from '../../../../HorizontalListSkeletons';

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
        history.push(PageRoutes.DATA_PRODUCTS);
    };

    return (
        <div id={HOME_PAGE_DATA_PRODUCTS_ID}>
            {loading && <HorizontalListSkeletons Component={SkeletonCard} />}
            {!loading && dataProducts && !!dataProducts.length && (
                <Section title="Data Products" actionText="View all" onClickAction={navigateToDataProducts}>
                    <Carousel>
                        {dataProducts.map((item) => {
                            const { dataProduct, domain } = item;
                            return <DataProductCard key={dataProduct.urn} dataProduct={dataProduct} domain={domain} />;
                        })}
                    </Carousel>
                </Section>
            )}
        </div>
    );
};
