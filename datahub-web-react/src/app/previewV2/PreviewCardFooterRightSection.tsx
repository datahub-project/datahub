import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { usePreviewData } from '@app/entityV2/shared/PreviewContext';
import SidebarPopularityHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarPopularityHeaderSection';
import {
    PopularityTier,
    getBarsStatusFromPopularityTier,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { DashboardLastUpdatedMs, DatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import Freshness from '@app/previewV2/Freshness';
import LineageBadge from '@app/previewV2/LineageBadge';
import QueryStat from '@app/previewV2/QueryStat';

import { DatasetStatsSummary, EntityType } from '@types';

const Container = styled.div`
    text-align: center;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: end;
`;

const StyledDivider = styled(Divider)`
    height: 16px;
    color: ${(props) => props.theme.colors.border};
`;

/** Matches LineageBadge's 16px icon so the footer does not shift when counts arrive. */
const LineageBadgePlaceholder = styled.div`
    width: 16px;
    height: 16px;
    flex-shrink: 0;
    visibility: hidden;
`;

interface Props {
    entityType: EntityType;
    urn: string;
    entityRegistry: EntityRegistry;
    showLineageBadge: boolean;
    /** Invisible slot for the deferred search badge while counts are in flight. */
    reserveLineageBadge?: boolean;
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    tier?: PopularityTier;
    statsSummary?: DatasetStatsSummary | null;
}

const PreviewCardFooterRightSection = ({
    entityType,
    urn,
    entityRegistry,
    showLineageBadge,
    reserveLineageBadge = false,
    lastUpdatedMs,
    tier,
    statsSummary,
}: Props) => {
    const { previewData } = usePreviewData();

    const status = tier !== undefined ? getBarsStatusFromPopularityTier(tier) : 0;
    const showLineageSlot = showLineageBadge || reserveLineageBadge;

    return (
        <>
            <Container>
                {!!statsSummary?.queryCountLast30Days && (
                    <>
                        <QueryStat queryCountLast30Days={statsSummary?.queryCountLast30Days} />
                        {showLineageSlot && <StyledDivider type="vertical" />}
                    </>
                )}
                {showLineageBadge && (
                    <LineageBadge
                        upstreamTotal={(previewData?.upstream?.total || 0) - (previewData?.upstream?.filtered || 0)}
                        downstreamTotal={
                            (previewData?.downstream?.total || 0) - (previewData?.downstream?.filtered || 0)
                        }
                        entityRegistry={entityRegistry}
                        entityType={entityType}
                        urn={urn}
                    />
                )}
                {reserveLineageBadge && !showLineageBadge && (
                    <LineageBadgePlaceholder data-testid="lineage-badge-placeholder" />
                )}
                {!!lastUpdatedMs?.lastUpdatedMs && (
                    <>
                        <StyledDivider type="vertical" />
                        <Freshness
                            time={lastUpdatedMs.lastUpdatedMs}
                            timeProperty={lastUpdatedMs.property}
                            showDate={false}
                        />
                    </>
                )}
                {!!(tier !== undefined && status) && (
                    <>
                        <StyledDivider type="vertical" />
                        <SidebarPopularityHeaderSection
                            statsSummary={statsSummary}
                            entityType={entityType}
                            size="small"
                        />
                    </>
                )}
            </Container>
        </>
    );
};

export default PreviewCardFooterRightSection;
