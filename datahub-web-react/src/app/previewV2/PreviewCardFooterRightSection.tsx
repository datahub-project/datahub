import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { DatasetStatsSummary, EntityType } from '../../types.generated';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { EntityCapabilityType } from '../entityV2/Entity';
import { usePreviewData } from '../entityV2/shared/PreviewContext';
import { entityHasCapability } from './utils';
import EntityRegistry from '../entityV2/EntityRegistry';
import LineageBadge from './LineageBadge';
import Freshness from './Freshness';
import {
    PopularityTier,
    getBarsStatusFromPopularityTier,
} from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import QueryStat from './QueryStat';
import SidebarPopularityHeaderSection from '../entityV2/shared/containers/profile/sidebar/shared/SidebarPopularityHeaderSection';

import { DatasetLastUpdatedMs, DashboardLastUpdatedMs } from '../entityV2/shared/utils';

const Container = styled.div`
    text-align: center;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: end;
`;

const StyledDivider = styled(Divider)`
    height: 16px;
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_2};
`;

interface Props {
    entityType: EntityType;
    urn: string;
    entityRegistry: EntityRegistry;
    entityCapabilities: Set<EntityCapabilityType>;
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    tier?: PopularityTier;
    statsSummary?: DatasetStatsSummary | null;
}

const PreviewCardFooterRightSection = ({
    entityType,
    urn,
    entityRegistry,
    entityCapabilities,
    lastUpdatedMs,
    tier,
    statsSummary,
}: Props) => {
    const previewData = usePreviewData();

    const status = tier !== undefined ? getBarsStatusFromPopularityTier(tier) : 0;
    const showLineageBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);

    return (
        <>
            <Container>
                {!!statsSummary?.queryCountLast30Days && (
                    <>
                        <QueryStat queryCountLast30Days={statsSummary?.queryCountLast30Days} />
                        {showLineageBadge && <StyledDivider type="vertical" />}
                    </>
                )}
                {showLineageBadge && (
                    <>
                        <LineageBadge
                            upstreamTotal={(previewData?.upstream?.total || 0) - (previewData?.upstream?.filtered || 0)}
                            downstreamTotal={
                                (previewData?.downstream?.total || 0) - (previewData?.downstream?.filtered || 0)
                            }
                            entityRegistry={entityRegistry}
                            entityType={entityType}
                            urn={urn}
                        />
                    </>
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
