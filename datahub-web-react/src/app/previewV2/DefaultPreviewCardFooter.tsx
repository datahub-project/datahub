import React from 'react';
import styled from 'styled-components';
import Pills from './Pills';
import { ANTD_GRAY } from '../entityV2/shared/constants';
import { PopularityBars } from '../entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { GlobalTags, GlossaryTerms, Maybe, Owner } from '../../types.generated';
import { EntityCapabilityType, PreviewType } from '../entityV2/Entity';

interface DefaultPreviewCardFooterProps {
    glossaryTerms?: GlossaryTerms;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    entityCapabilities: Set<EntityCapabilityType>;
    tier?: PopularityTier;
    status?: number;
    entityTitleSuffix?: React.ReactNode;
    previewType: Maybe<PreviewType> | undefined;
}

const Container = styled.div`
    border-top: 1px solid ${ANTD_GRAY[4]};
    padding-top: 12px;
    margin-bottom: -6px;
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: start;
`;

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

const DefaultPreviewCardFooter: React.FC<DefaultPreviewCardFooterProps> = ({
    glossaryTerms,
    tags,
    owners,
    entityCapabilities,
    tier,
    status,
    entityTitleSuffix,
    previewType,
}) => {
    const shouldRenderPillsRow = [glossaryTerms?.terms, tags?.tags, owners?.length].some(Boolean);
    const shouldRenderTier = tier !== undefined;
    const shouldRenderEntityLink = previewType === PreviewType.HOVER_CARD && entityTitleSuffix;

    return shouldRenderPillsRow || shouldRenderTier || shouldRenderEntityLink ? (
        <Container>
            <Pills glossaryTerms={glossaryTerms} tags={tags} owners={owners} entityCapabilities={entityCapabilities} />
            {previewType === PreviewType.HOVER_CARD && <EntityLink>{entityTitleSuffix}</EntityLink>}
            {tier !== undefined && status && <PopularityBars status={status} />}
        </Container>
    ) : null;
};

export default DefaultPreviewCardFooter;