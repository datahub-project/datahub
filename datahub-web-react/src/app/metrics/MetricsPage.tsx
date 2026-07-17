import { EmptyState } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import MetricsMainContent from '@app/metrics/MetricsMainContent';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

// Two-column flex shell: center content card + right entity sidebar.
// The left tree sidebar is owned by MetricsRoutes and rendered outside this
// component, so it is not duplicated when /metrics is the active route.
const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
    gap: 8px;
`;

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
 * Renders content-only (main card + right-rail placeholder).
 * The left tree sidebar is mounted by MetricsRoutes, mirroring
 * the ContextRoutes / ContextDocumentsPage split.
 */
export default function MetricsPage() {
    const { t } = useTranslation('misc');
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <ContentWrapper data-testid="metrics-page">
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
