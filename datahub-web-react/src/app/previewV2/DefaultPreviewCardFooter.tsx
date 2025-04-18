import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import Pills from './Pills';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import {
    DatasetStatsSummary,
    EntityPath,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Maybe,
    Owner,
} from '../../types.generated';
import { EntityCapabilityType, PreviewType } from '../entityV2/Entity';
import PreviewCardFooterRightSection from './PreviewCardFooterRightSection';
import EntityRegistry from '../entityV2/EntityRegistry';
import { entityHasCapability } from './utils';

import { DatasetLastUpdatedMs, DashboardLastUpdatedMs } from '../entityV2/shared/utils';

interface DefaultPreviewCardFooterProps {
    glossaryTerms?: GlossaryTerms;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    entityCapabilities: Set<EntityCapabilityType>;
    tier?: PopularityTier;
    entityTitleSuffix?: React.ReactNode;
    previewType: Maybe<PreviewType> | undefined;
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

const EntityLink = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;

    .ant-btn-link {
        display: flex;
        align-items: center;
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        height: 100%;

        :hover {
            color: ${REDESIGN_COLORS.HOVER_PURPLE};
        }

        > span:first-child {
            display: flex;
            align-items: center;
            height: 100%;
            line-height: normal;
        }
    }
`;

const HorizontalDivider = styled(Divider)`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_2};
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
    entityTitleSuffix,
    previewType,
    entityType,
    urn,
    entityRegistry,
    lastUpdatedMs,
    statsSummary,
    paths,
    isFullViewCard,
}) => {
    const shouldRenderPillsRow = [glossaryTerms?.terms, tags?.tags, owners?.length].some(Boolean);
    const shouldRenderEntityLink = previewType === PreviewType.HOVER_CARD && entityTitleSuffix;
    const shouldRenderRightSection =
        tier !== undefined ||
        lastUpdatedMs?.lastUpdatedMs ||
        statsSummary?.queryCountLast30Days ||
        entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);

    return shouldRenderPillsRow || shouldRenderRightSection || shouldRenderEntityLink ? (
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
                        entityCapabilities={entityCapabilities}
                        lastUpdatedMs={lastUpdatedMs}
                        tier={tier}
                        statsSummary={statsSummary}
                    />
                    {previewType === PreviewType.HOVER_CARD && <EntityLink>{entityTitleSuffix}</EntityLink>}
                </RightSection>
            </Container>
        </>
    ) : null;
};

export default DefaultPreviewCardFooter;
