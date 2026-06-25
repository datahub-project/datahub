import { EmptyState } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import MetricsMainContent from '@app/metrics/MetricsMainContent';
import MetricsSidebar, { SIDEBAR_COLLAPSED_WIDTH } from '@app/metrics/MetricsSidebar';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

// Three-column flex shell. 8px gap between the left sidebar, the center
// content card, and the right entity sidebar; no outer padding so the
// columns sit flush against the SearchablePage chrome.
const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
    gap: 8px;
`;

// Right-rail placeholder. Visual chrome matches `StyledEntitySidebarContainer`
// in `app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar.tsx`
// so the column reads as the same kind of entity sidebar as a Document /
// Dataset profile.
//
// Once `EntityType.Metric` exists in the GraphQL schema (waiting on Ani's
// data model PR), delete this placeholder and add a `MetricEntity.tsx` that
// mirrors `GlossaryTermEntity.tsx` — declare `getSidebarSections()` +
// `getSidebarTabs()` and let the shared `EntityProfile` framework render
// the real sidebar (Summary / Properties / Ask DataHub in Cloud) for free.
// No new "MetricsEntitySidebar" file required.
const EntitySidebarPlaceholder = styled.aside<{ $isShowNavBarRedesign?: boolean }>`
    flex: 0 0 400px;
    background-color: ${(props) => props.theme.colors.bg};
    overflow: auto;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 32px 24px;
    ${(props) =>
        props.$isShowNavBarRedesign && `border-radius: ${props.theme.styles['border-radius-navbar-redesign']};`}

    &::-webkit-scrollbar {
        display: none;
    }
`;

/**
 * MetricsPage - Stub landing page at `/metrics`.
 *
 * Layout (mirrors `ContextRoutes` + `DocumentNativeProfile`):
 *   [ left tree sidebar ] [ main content card ] [ right entity sidebar ]
 *
 * This is intentionally an empty-state stub: the backend Metric / Semantic
 * Model entities are not yet defined (Ani is revising the data model). Once
 * the GraphQL types land, the right rail should be replaced by the real
 * `EntityProfileSidebar` (wired automatically by registering a
 * `MetricEntity.tsx` in `buildEntityRegistryV2.ts`).
 */
export default function MetricsPage() {
    const { t } = useTranslation('misc');
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
    const expandedSidebarWidth = useSidebarWidth(0.2);

    const toggleSidebarCollapsed = useCallback(() => {
        setIsSidebarCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsSidebarCollapsed(false);
    }, []);

    const sidebarWidth = isSidebarCollapsed ? SIDEBAR_COLLAPSED_WIDTH : expandedSidebarWidth;

    return (
        <ContentWrapper data-testid="metrics-page">
            <MetricsSidebar
                width={sidebarWidth}
                isCollapsed={isSidebarCollapsed}
                onToggleCollapsed={toggleSidebarCollapsed}
                onExpandSidebar={expandSidebar}
            />
            <MetricsMainContent />
            <EntitySidebarPlaceholder $isShowNavBarRedesign={isShowNavBarRedesign} data-testid="metrics-entity-sidebar">
                <EmptyState
                    icon={Info}
                    title={t('metrics.entitySidebarEmptyTitle')}
                    description={t('metrics.entitySidebarEmptyDescription')}
                    size="sm"
                />
            </EntitySidebarPlaceholder>
        </ContentWrapper>
    );
}
