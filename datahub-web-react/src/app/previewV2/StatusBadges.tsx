import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { EntityType, Health } from '../../types.generated';
import { SEARCH_COLORS } from '../entityV2/shared/constants';
import { EntityCapabilityType } from '../entityV2/Entity';
import { entityHasCapability } from './utils';
import EntityRegistry from '../entityV2/EntityRegistry';
import LastUpdated from '../shared/LastUpdated';
import LineageBadge from './LineageBadge';
import HealthBadge from './HealthBadge';

const Container = styled.div`
    width: 100%;
    height: 22px;
    border-radius: 21px;
    background-color: ${SEARCH_COLORS.BACKGROUND_PURPLE};
    text-align: center;
    display: flex;
    flex-direction: row;
    gap: 6px;
    padding-left: 6px;
    padding-right: 6px;
    justify-content: center;
    align-items: center;
    & svg {
        font-size: 14px;
        color: #b0a2c2;
    }
`;

interface StatusBadgeProps {
    upstreamTotal: number | undefined;
    downstreamTotal: number | undefined;
    health: Health[] | undefined;
    entityType: EntityType;
    urn: string;
    url: string;
    history: ReturnType<typeof useHistory>;
    entityRegistry: EntityRegistry;
    entityCapabilities: Set<EntityCapabilityType>;
    lastUpdatedMs?: number | null;
    finalType: string | undefined;
    platform?: string;
    logoUrl?: string;
}

const StatusBadges: React.FC<StatusBadgeProps> = ({
    upstreamTotal,
    downstreamTotal,
    health,
    entityType,
    urn,
    url,
    history,
    entityRegistry,
    entityCapabilities,
    lastUpdatedMs,
    finalType,
    platform,
    logoUrl,
}) => {
    const showLineageBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);
    const showHealthBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.HEALTH);

    return (
        <>
            {(showLineageBadge || showHealthBadge || !!lastUpdatedMs) && (
                <Container>
                    {showLineageBadge && (
                        <LineageBadge
                            upstreamTotal={upstreamTotal}
                            downstreamTotal={downstreamTotal}
                            history={history}
                            entityRegistry={entityRegistry}
                            entityType={entityType}
                            urn={urn}
                        />
                    )}
                    {showHealthBadge && <HealthBadge health={health} url={url} />}
                    {!!lastUpdatedMs && (
                        <LastUpdated
                            noLabel
                            time={lastUpdatedMs}
                            typeName={finalType}
                            platformName={platform}
                            platformLogoUrl={logoUrl}
                        />
                    )}
                </Container>
            )}
        </>
    );
};

export default StatusBadges;
