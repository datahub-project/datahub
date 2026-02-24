import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { usePreviewData } from '@app/entityV2/shared/PreviewContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
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
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_2};
`;

interface Props {
    entityType: EntityType;
    urn: string;
    entityRegistry: EntityRegistry;
    showLineageBadge: boolean;
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    tier?: PopularityTier;
    statsSummary?: DatasetStatsSummary | null;
}

const PreviewCardFooterRightSection = ({
    entityType,
    urn,
    entityRegistry,
    showLineageBadge,
    lastUpdatedMs,
    tier,
    statsSummary,
}: Props) => {
    const previewData = usePreviewData();

    const status = tier !== undefined ? getBarsStatusFromPopularityTier(tier) : 0;

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
