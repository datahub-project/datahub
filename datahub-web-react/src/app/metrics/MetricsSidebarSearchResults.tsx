import { Button, Loader, Text } from '@components';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { MetricEntity } from '@app/metrics/metricsTypes';
import { PageRoutes } from '@conf/Global';

const ResultsWrap = styled.div<{ $dimmed?: boolean }>`
    display: flex;
    flex-direction: column;
    gap: 2px;
    opacity: ${(props) => (props.$dimmed ? 0.6 : 1)};
    transition: opacity 0.15s ease;
`;

const ResultsHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 0 8px 8px;
`;

const StateWrap = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    padding: 24px 16px;
`;

type Props = {
    metrics: MetricEntity[];
    /** Index total (may be > metrics.length when more pages are available). */
    total: number;
    loading: boolean;
    isRefreshing?: boolean;
    selectedUrn?: string | null;
    scrollRef?: (node?: Element | null) => void;
    onClear: () => void;
};

/**
 * Flat search results for metrics sidebar search mode (query and/or filters).
 * Uses MetricsTreeItem so rows match the browse tree chrome.
 */
export default function MetricsSidebarSearchResults({
    metrics,
    total,
    loading,
    isRefreshing = false,
    selectedUrn,
    scrollRef,
    onClear,
}: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();

    if (loading && metrics.length === 0) {
        return (
            <StateWrap>
                <Loader size="md" />
            </StateWrap>
        );
    }

    if (metrics.length === 0) {
        return (
            <StateWrap>
                <Text size="sm" color="gray">
                    {tc('noResults')}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid="metrics-sidebar-clear-search">
                    {t('context.clearSearch')}
                </Button>
            </StateWrap>
        );
    }

    const countLabel =
        total > metrics.length
            ? t('context.searchResultsCountTruncated', { count: metrics.length, total })
            : t('context.searchResultsCount', { count: total > 0 ? total : metrics.length });

    return (
        <ResultsWrap data-testid="metrics-sidebar-search-results" $dimmed={isRefreshing}>
            <ResultsHeader>
                <Text size="sm" color="gray">
                    {countLabel}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid="metrics-sidebar-clear-search">
                    {t('context.clearSearch')}
                </Button>
            </ResultsHeader>
            {metrics.map((metric) => {
                const title = metric.info?.name ?? metric.urn;
                return (
                    <MetricsTreeItem
                        key={metric.urn}
                        level={0}
                        icon={Sigma}
                        title={title}
                        isSelected={metric.urn === selectedUrn}
                        hasChildren={false}
                        isExpanded={false}
                        onClick={() => history.push(`${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`)}
                        testId={`metrics-sidebar-search-metric-${metric.urn}`}
                    />
                );
            })}
            {scrollRef ? <div ref={scrollRef} style={{ height: 1 }} /> : null}
        </ResultsWrap>
    );
}
