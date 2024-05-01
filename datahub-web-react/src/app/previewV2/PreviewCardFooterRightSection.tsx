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
import { PopularityBars } from '../entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';
import {
    PopularityTier,
    getBarsStatusFromPopularityTier,
} from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import QueryStat from './QueryStat';

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
    lastUpdatedMs?: { property?: string, lastUpdatedMs?: number };
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
                            upstreamTotal={previewData?.upstream?.total}
                            downstreamTotal={previewData?.downstream?.total}
                            entityRegistry={entityRegistry}
                            entityType={entityType}
                            urn={urn}
                        />
                        {!!lastUpdatedMs?.lastUpdatedMs && <StyledDivider type="vertical" />}
                    </>
                )}
                {!!lastUpdatedMs?.lastUpdatedMs && (
                    <Freshness
                        time={lastUpdatedMs.lastUpdatedMs}
                        timeProperty={lastUpdatedMs.property}
                        showDate={false}
                    />
                )}
                {!!(tier !== undefined && status) && (
                    <>
                        <StyledDivider type="vertical" />
                        <PopularityBars status={status} size="small" />
                    </>
                )}
            </Container>
        </>
    );
};

export default PreviewCardFooterRightSection;
