import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { useHistory } from 'react-router';
import Pills from './Pills';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { DatasetStatsSummary, EntityType, GlobalTags, GlossaryTerms, Maybe, Owner } from '../../types.generated';
import { EntityCapabilityType, PreviewType } from '../entityV2/Entity';
import PreviewCardFooterRightSection from './PreviewCardFooterRightSection';
import EntityRegistry from '../entityV2/EntityRegistry';
import { entityHasCapability } from './utils';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

interface DefaultPreviewCardFooterProps {
    glossaryTerms?: GlossaryTerms;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    entityCapabilities: Set<EntityCapabilityType>;
    tier?: PopularityTier;
    entityTitleSuffix?: React.ReactNode;
    previewType: Maybe<PreviewType> | undefined;
    upstreamTotal: number | undefined;
    downstreamTotal: number | undefined;
    entityType: EntityType;
    urn: string;
    history: ReturnType<typeof useHistory>;
    entityRegistry: EntityRegistry;
    lastUpdatedMs?: number | null;
    statsSummary?: DatasetStatsSummary | null;
}

const Container = styled.div`
    margin-bottom: -6px;
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const RightSection = styled.div``;

const EntityLink = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;

    .ant-btn-link {
        display: flex;
        align-items: center;
        color: #56668e;
        height: 100%;

        :hover {
            color: #533fd1;
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
    upstreamTotal,
    downstreamTotal,
    entityType,
    urn,
    history,
    entityRegistry,
    lastUpdatedMs,
    statsSummary,
}) => {
    const shouldRenderPillsRow = [glossaryTerms?.terms, tags?.tags, owners?.length].some(Boolean);
    const shouldRenderEntityLink = previewType === PreviewType.HOVER_CARD && entityTitleSuffix;
    const shouldRenderRightSection =
        tier !== undefined ||
        lastUpdatedMs ||
        statsSummary?.queryCountLast30Days ||
        entityHasCapability(entityCapabilities, EntityCapabilityType.LINEAGE);

    return shouldRenderPillsRow || shouldRenderRightSection || shouldRenderEntityLink ? (
        <>
            <HorizontalDivider />

            <Container>
                <Pills
                    glossaryTerms={glossaryTerms}
                    tags={tags}
                    owners={owners}
                    entityCapabilities={entityCapabilities}
                />
                <RightSection>
                    <PreviewCardFooterRightSection
                        upstreamTotal={upstreamTotal}
                        downstreamTotal={downstreamTotal}
                        entityType={entityType}
                        urn={urn}
                        history={history}
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