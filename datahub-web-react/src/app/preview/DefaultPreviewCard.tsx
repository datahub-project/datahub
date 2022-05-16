import React, { ReactNode } from 'react';
import { Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import {
    GlobalTags,
    Owner,
    GlossaryTerms,
    SearchInsight,
    Container,
    Domain,
    ParentContainersResult,
} from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';

import AvatarsGroup from '../shared/avatar/AvatarsGroup';
import TagTermGroup from '../shared/tags/TagTermGroup';
import { ANTD_GRAY } from '../entity/shared/constants';
import NoMarkdownViewer from '../entity/shared/components/styled/StripMarkdownText';
import { getNumberWithOrdinal } from '../entity/shared/utils';
import { useEntityData } from '../entity/shared/EntityContext';
import PlatformContentView from '../entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { useParentContainersTruncation } from '../entity/shared/containers/profile/header/PlatformContent/PlatformContentContainer';
import EntityCount from '../entity/shared/containers/profile/header/EntityCount';

const PreviewContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const PreviewWrapper = styled.div`
    width: 100%;
`;

const TitleContainer = styled.div`
    margin-bottom: 5px;
    line-height: 30px;

    .entityCount {
        margin-bottom: 2px;
    }
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    display: block;

    &&& {
        margin-right 8px;
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 600;
        vertical-align: middle;
    }
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
    vertical-align: text-top;
`;

const DescriptionContainer = styled.div`
    color: ${ANTD_GRAY[7]};
    margin-bottom: 8px;
`;

const AvatarContainer = styled.div`
    margin-right: 32px;
`;

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 3px;
`;

const TagSeparator = styled.div`
    margin: 2px 8px 0 0;
    height: 17px;
    border-right: 1px solid #cccccc;
`;

const InsightContainer = styled.div`
    margin-top: 12px;
`;

const InsightsText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 600;
    color: ${ANTD_GRAY[7]};
`;

const InsightIconContainer = styled.span`
    margin-right: 4px;
`;

interface Props {
    name: string;
    logoUrl?: string;
    logoComponent?: JSX.Element;
    url: string;
    description?: string;
    type?: string;
    typeIcon?: JSX.Element;
    platform?: string;
    platformInstanceId?: string;
    qualifier?: string | null;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    snippet?: React.ReactNode;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms;
    container?: Container;
    domain?: Domain | null;
    entityCount?: number;
    dataTestID?: string;
    titleSizePx?: number;
    onClick?: () => void;
    // this is provided by the impact analysis view. it is used to display
    // how the listed node is connected to the source node
    degree?: number;
    parentContainers?: ParentContainersResult | null;
}

export default function DefaultPreviewCard({
    name,
    logoUrl,
    logoComponent,
    url,
    description,
    type,
    typeIcon,
    platform,
    platformInstanceId,
    // TODO(Gabe): support qualifier in the new preview card
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    qualifier,
    tags,
    owners,
    snippet,
    insights,
    glossaryTerms,
    domain,
    container,
    entityCount,
    titleSizePx,
    dataTestID,
    onClick,
    degree,
    parentContainers,
}: Props) {
    // sometimes these lists will be rendered inside an entity container (for example, in the case of impact analysis)
    // in those cases, we may want to enrich the preview w/ context about the container entity
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const insightViews: Array<ReactNode> = [
        ...(insights?.map((insight) => (
            <>
                <InsightIconContainer>{insight.icon}</InsightIconContainer>
                <InsightsText>{insight.text}</InsightsText>
            </>
        )) || []),
    ];
    const hasGlossaryTerms = !!glossaryTerms?.terms?.length;
    const hasTags = !!tags?.tags?.length;
    if (snippet) {
        insightViews.push(snippet);
    }

    const { parentContainersRef, areContainersTruncated } = useParentContainersTruncation(container);

    return (
        <PreviewContainer data-testid={dataTestID}>
            <PreviewWrapper>
                <TitleContainer>
                    <Link to={url}>
                        <PlatformContentView
                            platformName={platform}
                            platformLogoUrl={logoUrl}
                            entityLogoComponent={logoComponent}
                            instanceId={platformInstanceId}
                            typeIcon={typeIcon}
                            entityType={type}
                            parentContainers={parentContainers?.containers}
                            parentContainersRef={parentContainersRef}
                            areContainersTruncated={areContainersTruncated}
                        />
                        <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                            {name || ' '}
                        </EntityTitle>
                        {degree !== undefined && degree !== null && (
                            <Tooltip
                                title={`This entity is a ${getNumberWithOrdinal(degree)} degree connection to ${
                                    entityData?.name || 'the source entity'
                                }`}
                            >
                                <PlatformText>{getNumberWithOrdinal(degree)}</PlatformText>
                            </Tooltip>
                        )}
                        {!!degree && entityCount && <PlatformDivider />}
                        <EntityCount entityCount={entityCount} />
                    </Link>
                </TitleContainer>
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer limit={250}>{description}</NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {(domain || hasGlossaryTerms || hasTags) && (
                    <TagContainer>
                        <TagTermGroup domain={domain} maxShow={3} />
                        {domain && hasGlossaryTerms && <TagSeparator />}
                        <TagTermGroup uneditableGlossaryTerms={glossaryTerms} maxShow={3} />
                        {((hasGlossaryTerms && hasTags) || (domain && hasTags)) && <TagSeparator />}
                        <TagTermGroup uneditableTags={tags} maxShow={3} />
                    </TagContainer>
                )}
                {owners && owners.length > 0 && (
                    <AvatarContainer>
                        <AvatarsGroup size={28} owners={owners} entityRegistry={entityRegistry} maxCount={4} />
                    </AvatarContainer>
                )}
                {insightViews.length > 0 && (
                    <InsightContainer>
                        {insightViews.map((insightView, index) => (
                            <span>
                                {insightView}
                                {index < insightViews.length - 1 && <PlatformDivider />}
                            </span>
                        ))}
                    </InsightContainer>
                )}
            </PreviewWrapper>
        </PreviewContainer>
    );
}
