import { Button, Card, EmptyState, borders } from '@components';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { ModuleHeader } from '@app/homeV3/module/components/LargeModule';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { PageRoutes } from '@conf/Global';

import {
    GetRootMetricsBrowseQuery,
    GetSemanticModelsBrowseQuery,
    useGetRootMetricsBrowseQuery,
    useGetSemanticModelsBrowseQuery,
} from '@graphql/metricsBrowse.generated';
import { Entity } from '@types';

// Cap the recent lists at the same count the home modules use, with a
// "show more" toggle — mirrors `EntityLinkList` / `AssetsYouOwn`.
const MAX_RECENT = 5;

const ContentCard = styled.div`
    flex: 1;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding: 16px 20px;
    gap: 8px;
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

const RecentLists = styled.div`
    display: flex;
    gap: 8px;
    flex: 1;
    overflow: hidden;
    flex-wrap: wrap;
`;

// Content area mirrors homeV3's `Content`: fixed height + scroll so the card
// matches the home module dimensions. `$hasFooter` reserves room for the
// "show more" row, exactly like `Content`'s `$hasViewAll`.
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

// Non-draggable header: reuse the home `ModuleHeader` but neutralize its
// hover highlight (which exists to signal draggability on the home page).
const MetricsModuleHeader = styled(ModuleHeader)`
    &:hover {
        background: transparent;
        border-bottom: ${borders['1px']} ${(props) => props.theme.colors.bg};
    }
`;

// "Show more" mirrors homeV3's `ViewAllButton`: right-aligned alchemy link
// button sitting below the scrollable content.
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

type SemanticModel = NonNullable<
    NonNullable<GetSemanticModelsBrowseQuery['getSemanticModels']>['semanticModels'][number]
>;
type Metric = NonNullable<NonNullable<GetRootMetricsBrowseQuery['getRootMetrics']>['metrics'][number]>;

function platformDisplayName(
    platform:
        | {
              name?: string | null;
              info?: { displayName?: string | null } | null;
              properties?: { displayName?: string | null } | null;
          }
        | null
        | undefined,
): string | null {
    if (!platform) return null;
    return platform.properties?.displayName ?? platform.info?.displayName ?? platform.name ?? null;
}

/**
 * MetricsMainContent - Landing page at /metrics.
 *
 * Shows total counts + recent semantic models and metrics sorted by lastModified.
 */
export default function MetricsMainContent() {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const theme = useTheme();
    const cardIconStyles = { color: theme.colors.icon };

    // Reset sidebar entity-data so no tree rows auto-expand on the home page.
    const { setEntityData } = useMetricsEntityContext();
    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);
    const cardStyle = { flex: 1 };

    const { data: modelsData } = useGetSemanticModelsBrowseQuery({
        variables: { input: { count: 100, start: 0 } },
    });
    const { data: metricsData } = useGetRootMetricsBrowseQuery({
        variables: { input: { count: 100, start: 0 } },
    });

    const totalModels = modelsData?.getSemanticModels?.total ?? 0;
    const totalMetrics = metricsData?.getRootMetrics?.total ?? 0;

    const recentModels: SemanticModel[] = useMemo(() => {
        const models = modelsData?.getSemanticModels?.semanticModels ?? [];
        return [...models].sort((a, b) => (b.info?.lastModified?.time ?? 0) - (a.info?.lastModified?.time ?? 0));
    }, [modelsData]);

    const recentMetrics: Metric[] = useMemo(() => {
        const metrics = metricsData?.getRootMetrics?.metrics ?? [];
        return [...metrics].sort((a, b) => (b.info?.lastModified?.time ?? 0) - (a.info?.lastModified?.time ?? 0));
    }, [metricsData]);

    const [showAllModels, setShowAllModels] = useState(false);
    const [showAllMetrics, setShowAllMetrics] = useState(false);
    const visibleModels = showAllModels ? recentModels : recentModels.slice(0, MAX_RECENT);
    const visibleMetrics = showAllMetrics ? recentMetrics : recentMetrics.slice(0, MAX_RECENT);

    const sourcePlatformCount = useMemo(() => {
        const names = new Set<string>();
        recentModels.forEach((m) => {
            const name = platformDisplayName(m.platform);
            if (name) names.add(name);
        });
        recentMetrics.forEach((m) => {
            const name = platformDisplayName(m.platform);
            if (name) names.add(name);
        });
        return names.size;
    }, [recentModels, recentMetrics]);

    const latestUpdateLabel = useMemo(() => {
        const latest = Math.max(
            ...recentModels.map((m) => m.info?.lastModified?.time ?? 0),
            ...recentMetrics.map((m) => m.info?.lastModified?.time ?? 0),
            0,
        );
        return latest > 0 ? toRelativeTimeString(latest) : null;
    }, [recentModels, recentMetrics]);

    const isEmpty = totalModels === 0 && totalMetrics === 0;

    return (
        <ContentCard data-testid="metrics-main-content">
            <PageHeader>
                <HomeTitle>{t('metrics.homeTitle')}</HomeTitle>
                <HomeBlurb>{t('metrics.homeBlurb')}</HomeBlurb>
            </PageHeader>

            {isEmpty ? (
                <EmptyListHint>
                    <EmptyState
                        icon={Sigma}
                        title={t('metrics.homeEmptyTitle')}
                        description={t('metrics.homeEmptyDescription')}
                        size="lg"
                        action={{
                            label: t('metrics.homeEmptyAction'),
                            onClick: () => history.push(PageRoutes.INGESTION_CREATE),
                            dataTestId: 'metrics-ingest-cta',
                        }}
                    />
                </EmptyListHint>
            ) : (
                <>
                    <SummaryCards>
                        <Card
                            dataTestId="metrics-count-models"
                            icon={<Cube size={18} weight="regular" />}
                            iconStyles={cardIconStyles}
                            style={cardStyle}
                            title={totalModels}
                            subTitle={t('metrics.totalSemanticModels')}
                        />
                        <Card
                            dataTestId="metrics-count-metrics"
                            icon={<Sigma size={18} weight="regular" />}
                            iconStyles={cardIconStyles}
                            style={cardStyle}
                            title={totalMetrics}
                            subTitle={t('metrics.totalMetrics')}
                        />
                        <Card
                            dataTestId="metrics-count-platforms"
                            icon={<Database size={18} weight="regular" />}
                            iconStyles={cardIconStyles}
                            style={cardStyle}
                            title={sourcePlatformCount}
                            subTitle={t('metrics.sourcePlatforms')}
                        />
                        <Card
                            dataTestId="metrics-latest-update"
                            icon={<Clock size={18} weight="regular" />}
                            iconStyles={cardIconStyles}
                            style={cardStyle}
                            title={latestUpdateLabel ?? '—'}
                            subTitle={t('metrics.latestUpdate')}
                        />
                    </SummaryCards>

                    <RecentLists>
                        <ModuleContainer
                            $height="316px"
                            data-testid="metrics-recent-models"
                            style={{ flex: 1, minWidth: 240 }}
                        >
                            <MetricsModuleHeader>
                                <ModuleName text={t('metrics.recentSemanticModels')} />
                            </MetricsModuleHeader>
                            <ModuleContent $hasFooter={recentModels.length > MAX_RECENT}>
                                {recentModels.length === 0 ? (
                                    <EmptyListHint>
                                        <EmptyState icon={Cube} title={t('metrics.emptyTreeTitle')} size="sm" />
                                    </EmptyListHint>
                                ) : (
                                    visibleModels.map((model) => (
                                        <AutoCompleteEntityItem
                                            key={model.urn}
                                            entity={model as unknown as Entity}
                                            customOnEntityClick={() =>
                                                history.push(
                                                    `${PageRoutes.SEMANTIC_MODEL_ENTITY}/${encodeURIComponent(model.urn)}`,
                                                )
                                            }
                                            hideMatches
                                            dataTestId={`recent-model-${model.urn}`}
                                        />
                                    ))
                                )}
                            </ModuleContent>
                            {recentModels.length > MAX_RECENT && (
                                <ShowMoreButton
                                    variant="link"
                                    color="gray"
                                    size="sm"
                                    onClick={() => setShowAllModels((v) => !v)}
                                >
                                    {showAllModels
                                        ? tc('showLess')
                                        : tc('showCountMore', { count: recentModels.length - MAX_RECENT })}
                                </ShowMoreButton>
                            )}
                        </ModuleContainer>

                        <ModuleContainer
                            $height="316px"
                            data-testid="metrics-recent-metrics"
                            style={{ flex: 1, minWidth: 240 }}
                        >
                            <MetricsModuleHeader>
                                <ModuleName text={t('metrics.recentMetrics')} />
                            </MetricsModuleHeader>
                            <ModuleContent $hasFooter={recentMetrics.length > MAX_RECENT}>
                                {recentMetrics.length === 0 ? (
                                    <EmptyListHint>
                                        <EmptyState icon={Sigma} title={t('metrics.emptyTreeTitle')} size="sm" />
                                    </EmptyListHint>
                                ) : (
                                    visibleMetrics.map((metric) => (
                                        <AutoCompleteEntityItem
                                            key={metric.urn}
                                            entity={metric as unknown as Entity}
                                            customOnEntityClick={() =>
                                                history.push(
                                                    `${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`,
                                                )
                                            }
                                            hideMatches
                                            dataTestId={`recent-metric-${metric.urn}`}
                                        />
                                    ))
                                )}
                            </ModuleContent>
                            {recentMetrics.length > MAX_RECENT && (
                                <ShowMoreButton
                                    variant="link"
                                    color="gray"
                                    size="sm"
                                    onClick={() => setShowAllMetrics((v) => !v)}
                                >
                                    {showAllMetrics
                                        ? tc('showLess')
                                        : tc('showCountMore', { count: recentMetrics.length - MAX_RECENT })}
                                </ShowMoreButton>
                            )}
                        </ModuleContainer>
                    </RecentLists>
                </>
            )}
        </ContentCard>
    );
}
