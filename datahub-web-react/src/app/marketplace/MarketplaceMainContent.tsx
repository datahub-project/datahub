import { Button, Card, EmptyState, Loader } from '@components';
import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import MarketplaceDataProductCard from '@app/marketplace/MarketplaceDataProductCard';
import { useMarketplaceEntityContext } from '@app/marketplace/context/MarketplaceEntityContext';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { PageRoutes } from '@conf/Global';

import {
    GetRootDataProductsBrowseQuery,
    useGetRootDataProductsBrowseQuery,
} from '@graphql/marketplaceBrowse.generated';

const MAX_RECENT = 6;

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

const HomeTitle = styled.div`
    font-size: 22px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.text};
`;

const HomeBlurb = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const SummaryCards = styled.div`
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
`;

const RecentSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const SectionTitle = styled.div`
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const CardGrid = styled.div`
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;

    @media (max-width: 900px) {
        grid-template-columns: 1fr;
    }
`;

const ShowMoreRow = styled.div`
    display: flex;
    justify-content: center;
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
 * Shows total counts + recent data products in a card grid.
 */
export default function MarketplaceMainContent() {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const theme = useTheme();
    const cardIconStyles = { color: theme.colors.icon };

    const { setEntityData } = useMarketplaceEntityContext();
    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);
    const cardStyle = { flex: 1 };

    // Follow-up: add sortInput to GetRootEntitiesInput / getRootDataProducts.
    const {
        data: productsData,
        loading,
        error,
    } = useGetRootDataProductsBrowseQuery({
        variables: { input: { count: 500, start: 0 } },
    });

    const totalProducts = productsData?.getRootDataProducts?.total ?? 0;

    const recentProducts: DataProduct[] = useMemo(() => {
        const products = productsData?.getRootDataProducts?.dataProducts ?? [];
        return [...products].sort(
            (a, b) => (b.properties?.createdOn?.time ?? 0) - (a.properties?.createdOn?.time ?? 0),
        );
    }, [productsData]);

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

    const latestAdditionLabel = useMemo(() => {
        const latest = Math.max(...recentProducts.map((p) => p.properties?.createdOn?.time ?? 0), 0);
        return latest > 0 ? toRelativeTimeString(latest) : null;
    }, [recentProducts]);

    const isEmpty = !loading && !error && totalProducts === 0;

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
                        onClick: () => history.push(PageRoutes.DOMAINS),
                        dataTestId: 'marketplace-domains-cta',
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
                        icon={<Storefront size={18} weight="regular" />}
                        iconStyles={cardIconStyles}
                        style={cardStyle}
                        title={String(totalProducts)}
                        subTitle={t('marketplace.totalDataProducts')}
                    />
                    <Card
                        dataTestId="marketplace-count-applications"
                        icon={<AppWindow size={18} weight="regular" />}
                        iconStyles={cardIconStyles}
                        style={cardStyle}
                        title={String(applicationCount)}
                        subTitle={t('marketplace.sourceApplications')}
                    />
                    <Card
                        dataTestId="marketplace-latest-update"
                        icon={<Clock size={18} weight="regular" />}
                        iconStyles={cardIconStyles}
                        style={cardStyle}
                        title={latestAdditionLabel ?? '—'}
                        subTitle={t('marketplace.latestAddition')}
                    />
                </SummaryCards>

                <RecentSection data-testid="marketplace-recent-products">
                    <SectionHeader>
                        <SectionTitle>{t('marketplace.recentDataProducts')}</SectionTitle>
                    </SectionHeader>
                    {recentProducts.length === 0 ? (
                        <EmptyListHint>
                            <EmptyState icon={Storefront} title={t('marketplace.emptyTreeTitle')} size="sm" />
                        </EmptyListHint>
                    ) : (
                        <>
                            <CardGrid>
                                {visibleProducts.map((product) => (
                                    <MarketplaceDataProductCard key={product.urn} dataProduct={product} />
                                ))}
                            </CardGrid>
                            {recentProducts.length > MAX_RECENT && (
                                <ShowMoreRow>
                                    <Button
                                        variant="link"
                                        color="gray"
                                        size="sm"
                                        onClick={() => setShowAllProducts((v) => !v)}
                                    >
                                        {showAllProducts
                                            ? tc('showLess')
                                            : tc('showCountMore', {
                                                  count: recentProducts.length - MAX_RECENT,
                                              })}
                                    </Button>
                                </ShowMoreRow>
                            )}
                        </>
                    )}
                </RecentSection>
            </>
        );
    }

    return (
        <ContentCard data-testid="marketplace-main-content">
            <PageHeader>
                <HomeTitle>{t('marketplace.homeTitle')}</HomeTitle>
                <HomeBlurb>{t('marketplace.homeBlurb')}</HomeBlurb>
            </PageHeader>
            {body}
        </ContentCard>
    );
}
