import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EntityCapabilityType, PreviewType } from '@app/entityV2/Entity';
import EntityRegistry from '@app/entityV2/EntityRegistry';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { DashboardLastUpdatedMs, DatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import Pills from '@app/previewV2/Pills';
import PreviewCardFooterRightSection from '@app/previewV2/PreviewCardFooterRightSection';
import { entityHasCapability } from '@app/previewV2/utils';
import { useHideLineageInSearchCards } from '@app/useAppConfig';

import { DatasetStatsSummary, EntityPath, EntityType, GlobalTags, GlossaryTerms, Maybe, Owner } from '@types';

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
        color: ${(props) => props.theme.styles['primary-color']};
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
    const hideLineage = useHideLineageInSearchCards();
    const showLineageBadge = !hideLineage && entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);

    const shouldRenderPillsRow = [glossaryTerms?.terms, tags?.tags, owners?.length].some(Boolean);
    const shouldRenderEntityLink = previewType === PreviewType.HOVER_CARD && entityTitleSuffix;
    const shouldRenderRightSection =
        tier !== undefined || lastUpdatedMs?.lastUpdatedMs || statsSummary?.queryCountLast30Days || showLineageBadge;

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
                        showLineageBadge={showLineageBadge}
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
