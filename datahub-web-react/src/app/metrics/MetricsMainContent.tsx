import { EmptyState } from '@components';
import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { PageRoutes } from '@conf/Global';

import {
    GetRootMetricsBrowseQuery,
    GetSemanticModelsBrowseQuery,
    useGetRootMetricsBrowseQuery,
    useGetSemanticModelsBrowseQuery,
} from '@graphql/metricsBrowse.generated';

const ContentCard = styled.div`
    flex: 1;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding: 32px 40px;
    gap: 24px;
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
    gap: 16px;
    flex-wrap: wrap;
`;

const CountCard = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    padding: 20px 24px;
    min-width: 160px;
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 10px;
    border: 1px solid ${(props) => props.theme.colors.border};
    gap: 4px;
`;

const CountNumber = styled.div`
    font-size: 32px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.text};
    line-height: 1.1;
`;

const CountLabel = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const CountIconRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    color: ${(props) => props.theme.colors.icon};
    margin-bottom: 4px;
`;

const RecentLists = styled.div`
    display: flex;
    gap: 24px;
    flex: 1;
    overflow: hidden;
    flex-wrap: wrap;
`;

const RecentList = styled.div`
    flex: 1;
    min-width: 240px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    overflow: hidden;
`;

const RecentListTitle = styled.div`
    font-size: 15px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

const RecentItems = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    overflow-y: auto;
    max-height: 320px;

    &::-webkit-scrollbar {
        width: 4px;
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 2px;
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} transparent;
`;

const RecentItem = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 12px;
    border-radius: 6px;
    cursor: pointer;
    transition: background 0.12s ease;
    border: 1px solid transparent;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        border-color: ${(props) => props.theme.colors.border};
    }
`;

const RecentItemIcon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.icon};
`;

const RecentItemText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    overflow: hidden;
`;

const RecentItemName = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const RecentItemMeta = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const EmptyListHint = styled.div`
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
    padding: 32px 16px;
`;

const Dot = styled.span`
    margin: 0 4px;
    color: ${(props) => props.theme.colors.textTertiary};

    &::before {
        content: '·';
    }
`;

type SemanticModel = NonNullable<
    NonNullable<GetSemanticModelsBrowseQuery['getSemanticModels']>['semanticModels'][number]
>;
type Metric = NonNullable<NonNullable<GetRootMetricsBrowseQuery['getRootMetrics']>['metrics'][number]>;

function ModelMeta({ platform, lastModified }: { platform: string | null; lastModified: number | null | undefined }) {
    const rel = toRelativeTimeString(lastModified ?? undefined);
    if (!platform && !rel) return null;
    if (!platform) return <>{rel}</>;
    if (!rel) return <>{platform}</>;
    return (
        <>
            {platform}
            <Dot />
            {rel}
        </>
    );
}

function MetricMeta({
    semanticModelName,
    lastModified,
}: {
    semanticModelName: string | null;
    lastModified: number | null | undefined;
}) {
    const rel = toRelativeTimeString(lastModified ?? undefined);
    if (!semanticModelName && !rel) return null;
    if (!semanticModelName) return <>{rel}</>;
    if (!rel) return <>{semanticModelName}</>;
    return (
        <>
            {semanticModelName}
            <Dot />
            {rel}
        </>
    );
}

/**
 * MetricsMainContent - Landing page at /metrics.
 *
 * Shows total counts + recent semantic models and metrics sorted by lastModified.
 */
export default function MetricsMainContent() {
    const { t } = useTranslation('misc');
    const history = useHistory();

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
                    />
                </EmptyListHint>
            ) : (
                <>
                    <SummaryCards>
                        <CountCard data-testid="metrics-count-models">
                            <CountIconRow>
                                <Cube size={18} weight="regular" />
                            </CountIconRow>
                            <CountNumber>{totalModels}</CountNumber>
                            <CountLabel>{t('metrics.totalSemanticModels')}</CountLabel>
                        </CountCard>
                        <CountCard data-testid="metrics-count-metrics">
                            <CountIconRow>
                                <Sigma size={18} weight="regular" />
                            </CountIconRow>
                            <CountNumber>{totalMetrics}</CountNumber>
                            <CountLabel>{t('metrics.totalMetrics')}</CountLabel>
                        </CountCard>
                    </SummaryCards>

                    <RecentLists>
                        <RecentList data-testid="metrics-recent-models">
                            <RecentListTitle>{t('metrics.recentSemanticModels')}</RecentListTitle>
                            {recentModels.length === 0 ? (
                                <EmptyListHint>
                                    <EmptyState icon={Cube} title={t('metrics.emptyTreeTitle')} size="sm" />
                                </EmptyListHint>
                            ) : (
                                <RecentItems>
                                    {recentModels.map((model) => (
                                        <RecentItem
                                            key={model.urn}
                                            onClick={() =>
                                                history.push(
                                                    `${PageRoutes.SEMANTIC_MODEL_ENTITY}/${encodeURIComponent(model.urn)}`,
                                                )
                                            }
                                            data-testid={`recent-model-${model.urn}`}
                                        >
                                            <RecentItemIcon>
                                                <Cube size={18} weight="regular" />
                                            </RecentItemIcon>
                                            <RecentItemText>
                                                <RecentItemName>{model.info?.name ?? model.urn}</RecentItemName>
                                                <RecentItemMeta>
                                                    <ModelMeta
                                                        platform={
                                                            model.platform?.properties?.displayName ??
                                                            model.platform?.info?.displayName ??
                                                            model.platform?.name ??
                                                            null
                                                        }
                                                        lastModified={model.info?.lastModified?.time}
                                                    />
                                                </RecentItemMeta>
                                            </RecentItemText>
                                        </RecentItem>
                                    ))}
                                </RecentItems>
                            )}
                        </RecentList>

                        <RecentList data-testid="metrics-recent-metrics">
                            <RecentListTitle>{t('metrics.recentMetrics')}</RecentListTitle>
                            {recentMetrics.length === 0 ? (
                                <EmptyListHint>
                                    <EmptyState icon={Sigma} title={t('metrics.emptyTreeTitle')} size="sm" />
                                </EmptyListHint>
                            ) : (
                                <RecentItems>
                                    {recentMetrics.map((metric) => (
                                        <RecentItem
                                            key={metric.urn}
                                            onClick={() =>
                                                history.push(
                                                    `${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`,
                                                )
                                            }
                                            data-testid={`recent-metric-${metric.urn}`}
                                        >
                                            <RecentItemIcon>
                                                <Sigma size={18} weight="regular" />
                                            </RecentItemIcon>
                                            <RecentItemText>
                                                <RecentItemName>{metric.info?.name ?? metric.urn}</RecentItemName>
                                                <RecentItemMeta>
                                                    <MetricMeta
                                                        semanticModelName={metric.semanticModel?.info?.name ?? null}
                                                        lastModified={metric.info?.lastModified?.time}
                                                    />
                                                </RecentItemMeta>
                                            </RecentItemText>
                                        </RecentItem>
                                    ))}
                                </RecentItems>
                            )}
                        </RecentList>
                    </RecentLists>
                </>
            )}
        </ContentCard>
    );
}
