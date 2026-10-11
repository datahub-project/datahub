import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import EntityRegistry from '@app/entityV2/EntityRegistry';
import { usePreviewData } from '@app/entityV2/shared/PreviewContext';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { DashboardLastUpdatedMs, DatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import Pills from '@app/previewV2/Pills';
import PreviewCardFooterRightSection from '@app/previewV2/PreviewCardFooterRightSection';
import { entityHasCapability } from '@app/previewV2/utils';
import { useSearchResult } from '@app/search/context/SearchResultContext';
import { useSearchResultLineageStatus } from '@app/searchV2/SearchResultLineageStatusContext';
import { useHideLineageInSearchCards } from '@app/useAppConfig';

import { DatasetStatsSummary, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

interface DefaultPreviewCardFooterProps {
    glossaryTerms?: GlossaryTerms;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    entityCapabilities: Set<EntityCapabilityType>;
    tier?: PopularityTier;
    entityType: EntityType;
    urn: string;
    entityRegistry: EntityRegistry;
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    statsSummary?: DatasetStatsSummary | null;
    paths?: EntityPath[];
    isFullViewCard?: boolean;
}

const Container = styled.div`
    margin-bottom: -6px;
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;

    .ant-btn-link {
        padding: inherit;
    }
`;

const RightSection = styled.div<{ isFullViewCard?: boolean }>`
    display: flex;
    justify-content: flex-end;
    width: 100%;
    padding: ${(props) => (props.isFullViewCard ? '0 10px' : '0 5px')};
`;

const HorizontalDivider = styled(Divider)`
    color: ${(props) => props.theme.colors.border};
    margin-top: 14px;
    margin-bottom: 8px;
    width: calc(100% + 40px) !important;
    margin-left: -20px;
`;

const DefaultPreviewCardFooter: React.FC<DefaultPreviewCardFooterProps> = ({
    glossaryTerms,
    tags,
    owners,
    entityCapabilities,
    tier,
    entityType,
    urn,
    entityRegistry,
    lastUpdatedMs,
    statsSummary,
    paths,
    isFullViewCard,
}) => {
    const { previewData } = usePreviewData();
    const hideLineage = useHideLineageInSearchCards();
    const { failed: lineageCountsFailed } = useSearchResultLineageStatus();
    // SearchResultProvider is only set for search cards; browse/preview keep capability-based badges.
    const isSearchResultCard = useSearchResult() != null;
    const hasLineageCapability = !hideLineage && entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);
    // Missing counts are not the same as zero. Search cards render before the deferred count
    // query returns; treat that gap as a reserved placeholder rather than a "no lineage" icon.
    // Drop the reservation when the page batch fails so slots do not stay empty forever.
    const hasLineageCounts = previewData?.upstream != null || previewData?.downstream != null;
    const showLineageBadge = hasLineageCapability && (!isSearchResultCard || hasLineageCounts);
    const reserveLineageBadge = isSearchResultCard && hasLineageCapability && !hasLineageCounts && !lineageCountsFailed;

    const shouldRenderPillsRow = [glossaryTerms?.terms, tags?.tags, owners?.length].some(Boolean);
    const shouldRenderRightSection =
        tier !== undefined ||
        lastUpdatedMs?.lastUpdatedMs ||
        statsSummary?.queryCountLast30Days ||
        showLineageBadge ||
        reserveLineageBadge;

    return shouldRenderPillsRow || shouldRenderRightSection ? (
        <>
            {isFullViewCard && <HorizontalDivider />}

            <Container>
                {isFullViewCard && (
                    <Pills
                        glossaryTerms={glossaryTerms}
                        tags={tags}
                        owners={owners}
                        entityCapabilities={entityCapabilities}
                        paths={paths}
                        entityType={entityType}
                    />
                )}
                <RightSection isFullViewCard={isFullViewCard}>
                    <PreviewCardFooterRightSection
                        entityType={entityType}
                        urn={urn}
                        entityRegistry={entityRegistry}
                        showLineageBadge={showLineageBadge}
                        reserveLineageBadge={reserveLineageBadge}
                        lastUpdatedMs={lastUpdatedMs}
                        tier={tier}
                        statsSummary={statsSummary}
                    />
                </RightSection>
            </Container>
        </>
    ) : null;
};

export default DefaultPreviewCardFooter;
