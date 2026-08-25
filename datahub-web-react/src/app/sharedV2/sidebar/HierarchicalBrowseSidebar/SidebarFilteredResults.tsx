import { Button, Loader, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

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
    /** Result rows (entity-specific). Rendered under the shared count / clear header. */
    children?: React.ReactNode;
    /** Rows currently on screen (may be capped by pagination). */
    count: number;
    /** Index total when larger than `count` (shows "Showing X of Y"). */
    total?: number;
    loading: boolean;
    /** Refetch in flight while previous hits are still visible. */
    isRefreshing?: boolean;
    onClear: () => void;
    clearTestId?: string;
    dataTestId?: string;
};

/**
 * Shared chrome for hierarchical browse sidebars in flat filter / search mode:
 * loading, empty ("No results found" + clear), and results header (count + clear).
 *
 * Documents, Glossary, and Domains should use this — do not re-implement the layout.
 */
export default function SidebarFilteredResults({
    children,
    count,
    total,
    loading,
    isRefreshing = false,
    onClear,
    clearTestId = 'sidebar-clear-filters',
    dataTestId = 'sidebar-filtered-results',
}: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');

    if (loading && count === 0) {
        return (
            <StateWrap>
                <Loader size="md" />
            </StateWrap>
        );
    }

    if (count === 0) {
        return (
            <StateWrap>
                <Text size="sm" color="gray">
                    {tc('noResults')}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid={clearTestId}>
                    {t('context.clearSearch')}
                </Button>
            </StateWrap>
        );
    }

    const resolvedTotal = total ?? count;
    const countLabel =
        resolvedTotal > count
            ? t('context.searchResultsCountTruncated', { count, total: resolvedTotal })
            : t('context.searchResultsCount', { count: resolvedTotal > 0 ? resolvedTotal : count });

    return (
        <ResultsWrap data-testid={dataTestId} $dimmed={isRefreshing}>
            <ResultsHeader>
                <Text size="sm" color="gray">
                    {countLabel}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid={clearTestId}>
                    {t('context.clearSearch')}
                </Button>
            </ResultsHeader>
            {children}
        </ResultsWrap>
    );
}
