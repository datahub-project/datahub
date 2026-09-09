import { Button, Card, EmptyState, Loader, PageTitle, borders } from '@components';
import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { ModuleHeader } from '@app/homeV3/module/components/LargeModule';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import { useMarketplaceEntityContext } from '@app/marketplace/context/MarketplaceEntityContext';
import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import {
    countPendingOptimistic,
    isRootDataProduct,
    mergeDataProductEntities,
} from '@app/marketplace/utils/marketplaceDataProductEntity';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { toCompactRelativeTimeString } from '@app/shared/time/timeUtils';
import { SummaryStatIconBadge } from '@app/sharedV2/cards/SummaryStatIconBadge';
import { PageRoutes } from '@conf/Global';

import {
    GetRootDataProductsBrowseQuery,
    useGetRootDataProductsBrowseQuery,
} from '@graphql/marketplaceBrowse.generated';
import { Entity } from '@types';

const MAX_RECENT = 5;

const ContentCard = styled.div`
    flex: 1;
    height: 100%;
    min-height: 0;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding: 16px 20px;
    gap: 12px;
`;

const PageHeader = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const SummaryCards = styled.div`
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
`;

const RecentProductsModule = styled(ModuleContainer)`
    flex: none;
    width: 100%;
`;

const MarketplaceModuleHeader = styled(ModuleHeader)`
    &:hover {
        background: transparent;
        border-bottom: ${borders['1px']} ${(props) => props.theme.colors.bg};
    }
`;

const ModuleContent = styled.div<{ $hasFooter?: boolean }>`
    display: flex;
    flex-direction: column;
    margin: 0 0 8px 8px;
    padding-right: 5px;
    overflow-y: auto;
    scrollbar-gutter: stable;
    height: ${(props) => (props.$hasFooter ? '234px' : '246px')};

    &::-webkit-scrollbar {
        width: 6px;
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} transparent;
`;

const ShowMoreButton = styled(Button)`
    margin: 0 16px 0 auto;
    padding-right: 8px;
`;

const EmptyListHint = styled.div`
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
    padding: 32px 16px;
`;

type DataProduct = NonNullable<
    NonNullable<GetRootDataProductsBrowseQuery['getRootDataProducts']>['dataProducts'][number]
>;

/**
 * MarketplaceMainContent - Landing page at /marketplace.
 *
 * Summary stats plus a single recent-data-products module (Metrics-style list rows).
 */
export default function MarketplaceMainContent() {
    const { t } = useTranslation('misc');
    const { t: te } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();

    const { setEntityData, refetchKey, openCreateModal, optimisticDataProducts, syncOptimisticWithIndexed } =
        useMarketplaceEntityContext();
    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);
    const cardStyle = { flex: 1 };

    const {
        data: productsData,
        loading,
        error,
        refetch,
    } = useGetRootDataProductsBrowseQuery({
        variables: { input: { count: 500, start: 0 } },
    });

    useEffect(() => {
        if (refetchKey > 0) {
            refetch();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refetchKey]);

    const totalProducts = productsData?.getRootDataProducts?.total ?? 0;
    const fetchedProducts = useMemo(
        () => productsData?.getRootDataProducts?.dataProducts ?? [],
        [productsData?.getRootDataProducts?.dataProducts],
    );

    useEffect(() => {
        syncOptimisticWithIndexed(fetchedProducts.map((p) => p.urn));
    }, [fetchedProducts, syncOptimisticWithIndexed]);

    const optimisticRootProducts = useMemo(
        () => optimisticDataProducts.filter(isRootDataProduct),
        [optimisticDataProducts],
    );
    const pendingOptimisticCount = useMemo(
        () => countPendingOptimistic(fetchedProducts, optimisticRootProducts),
        [fetchedProducts, optimisticRootProducts],
    );
    const displayedTotalProducts = totalProducts + pendingOptimisticCount;

    const recentProducts: DataProduct[] = useMemo(() => {
        const merged = mergeDataProductEntities(
            fetchedProducts as unknown as DataProductEntity[],
            optimisticRootProducts,
        ) as unknown as DataProduct[];
        return [...merged].sort((a, b) => (b.properties?.createdOn?.time ?? 0) - (a.properties?.createdOn?.time ?? 0));
    }, [fetchedProducts, optimisticRootProducts]);

    const [showAllProducts, setShowAllProducts] = useState(false);
    const visibleProducts = showAllProducts ? recentProducts : recentProducts.slice(0, MAX_RECENT);

    const applicationCount = useMemo(() => {
        const urns = new Set<string>();
        recentProducts.forEach((p) => {
            (p.applications ?? []).forEach((assoc) => {
                const urn = assoc.application?.urn;
                if (urn) urns.add(urn);
            });
        });
        return urns.size;
    }, [recentProducts]);

    const latestCreatedLabel = useMemo(() => {
        const latest = Math.max(...recentProducts.map((p) => p.properties?.createdOn?.time ?? 0), 0);
        return latest > 0 ? toCompactRelativeTimeString(latest) : null;
    }, [recentProducts]);

    const isEmpty = !loading && !error && displayedTotalProducts === 0;

    let body: React.ReactNode;
    if (loading) {
        body = (
            <EmptyListHint>
                <Loader size="lg" />
            </EmptyListHint>
        );
    } else if (error) {
        body = (
            <EmptyListHint>
                <EmptyState
                    icon={Storefront}
                    title={t('marketplace.homeLoadErrorTitle')}
                    description={t('marketplace.homeLoadErrorDescription')}
                    size="lg"
                />
            </EmptyListHint>
        );
    } else if (isEmpty) {
        body = (
            <EmptyListHint>
                <EmptyState
                    icon={Storefront}
                    title={t('marketplace.homeEmptyTitle')}
                    description={t('marketplace.homeEmptyDescription')}
                    size="lg"
                    action={{
                        label: t('marketplace.homeEmptyAction'),
                        onClick: openCreateModal,
                        dataTestId: 'marketplace-create-data-product-cta',
                    }}
                />
            </EmptyListHint>
        );
    } else {
        body = (
            <>
                <SummaryCards>
                    <Card
                        dataTestId="marketplace-count-products"
                        icon={<SummaryStatIconBadge icon={Storefront} tone="brand" />}
                        style={cardStyle}
                        title={String(displayedTotalProducts)}
                        subTitle={t('marketplace.totalDataProducts')}
                    />
                    <Card
                        dataTestId="marketplace-count-applications"
                        icon={<SummaryStatIconBadge icon={AppWindow} tone="info" />}
                        style={cardStyle}
                        title={String(applicationCount)}
                        subTitle={t('marketplace.sourceApplications')}
                    />
                    <Card
                        dataTestId="marketplace-latest-update"
                        icon={<SummaryStatIconBadge icon={Clock} tone="neutral" />}
                        style={cardStyle}
                        title={latestCreatedLabel ?? '—'}
                        subTitle={t('marketplace.lastCreated')}
                    />
                </SummaryCards>

                <RecentProductsModule $height="316px" data-testid="marketplace-recent-products">
                    <MarketplaceModuleHeader>
                        <ModuleName text={t('marketplace.recentDataProducts')} />
                    </MarketplaceModuleHeader>
                    <ModuleContent $hasFooter={recentProducts.length > MAX_RECENT}>
                        {recentProducts.length === 0 ? (
                            <EmptyListHint>
                                <EmptyState icon={Storefront} title={t('marketplace.emptyTreeTitle')} size="sm" />
                            </EmptyListHint>
                        ) : (
                            visibleProducts.map((product) => (
                                <AutoCompleteEntityItem
                                    key={product.urn}
                                    entity={product as unknown as Entity}
                                    customOnEntityClick={() =>
                                        history.push(
                                            `${PageRoutes.DATA_PRODUCT_ENTITY}/${encodeURIComponent(product.urn)}`,
                                        )
                                    }
                                    hideMatches
                                    dataTestId={`marketplace-recent-product-${product.urn}`}
                                />
                            ))
                        )}
                    </ModuleContent>
                    {recentProducts.length > MAX_RECENT && (
                        <ShowMoreButton
                            variant="link"
                            color="gray"
                            size="sm"
                            onClick={() => setShowAllProducts((v) => !v)}
                        >
                            {showAllProducts
                                ? tc('showLess')
                                : tc('showCountMore', { count: recentProducts.length - MAX_RECENT })}
                        </ShowMoreButton>
                    )}
                </RecentProductsModule>
            </>
        );
    }

    return (
        <ContentCard data-testid="marketplace-main-content">
            <PageHeader>
                <PageTitle
                    title={t('marketplace.homeTitle')}
                    subTitle={t('marketplace.homeBlurb')}
                    actionButton={{
                        label: te('dataProduct.createTitle'),
                        icon: { icon: Plus },
                        onClick: openCreateModal,
                    }}
                />
            </PageHeader>
            {body}
        </ContentCard>
    );
}
