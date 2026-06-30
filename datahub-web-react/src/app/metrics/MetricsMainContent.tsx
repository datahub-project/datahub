import { EmptyState } from '@components';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

// Center column card. Visual styling mirrors `ContentCard` in
// `app/entityV2/document/DocumentNativeProfile.tsx` so the Metrics page
// reads as the same kind of profile surface as a Document profile.
// No outer margin — gaps between the three columns are owned by the
// parent `ContentWrapper` in `MetricsPage.tsx`.
const ContentCard = styled.div`
    flex: 1;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    overflow: hidden;
    padding: 40px;
`;

/**
 * MetricsMainContent - Center column of the Metrics page (stub).
 *
 * Empty-state placeholder until a metric is selected. Once profile routes
 * exist (e.g. `/metric/:urn`), the parent will render the real profile here
 * instead of this placeholder.
 */
export default function MetricsMainContent() {
    const { t } = useTranslation('misc');

    return (
        <ContentCard data-testid="metrics-main-content-empty">
            <EmptyState
                icon={Sigma}
                title={t('metrics.emptySelectionTitle')}
                description={t('metrics.emptySelectionDescription')}
                size="lg"
            />
        </ContentCard>
    );
}
