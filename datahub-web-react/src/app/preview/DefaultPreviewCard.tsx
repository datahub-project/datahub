import { Image, Tooltip, Typography } from 'antd';
import React, { ReactNode } from 'react';
import { FolderOpenOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { GlobalTags, Owner, GlossaryTerms, SearchInsight, Container, EntityType, Domain } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';

import AvatarsGroup from '../shared/avatar/AvatarsGroup';
import TagTermGroup from '../shared/tags/TagTermGroup';
import { ANTD_GRAY } from '../entity/shared/constants';
import NoMarkdownViewer from '../entity/shared/components/styled/StripMarkdownText';
import { getNumberWithOrdinal } from '../entity/shared/utils';
import { useEntityData } from '../entity/shared/EntityContext';

const LogoContainer = styled.div`
    padding-right: 8px;
`;

const PreviewContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const PlatformInfo = styled.div`
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    height: 24px;
`;

const TitleContainer = styled.div`
    margin-bottom: 0px;
    line-height: 30px;
`;

const PreviewImage = styled(Image)`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    margin-right: 8px;
    background-color: transparent;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
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

const EntityCountText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
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
`;

const AvatarContainer = styled.div`
    margin-top: 6px;
    margin-right: 32px;
`;

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 5px;
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

const TypeIcon = styled.span`
    margin-right: 8px;
`;

const ContainerText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[9]};
`;

const ContainerIcon = styled(FolderOpenOutlined)`
    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
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
    if (snippet) {
        insightViews.push(snippet);
    }
    return (
        <PreviewContainer data-testid={dataTestID}>
            <div style={{ width: '100%' }}>
                <TitleContainer>
                    <Link to={url}>
                        <PlatformInfo>
                            {(logoUrl && <PreviewImage preview={false} src={logoUrl} alt={platform || ''} />) || (
                                <LogoContainer>{logoComponent}</LogoContainer>
                            )}
                            {platform && <PlatformText>{platform}</PlatformText>}
                            {(logoUrl || logoComponent || platform) && <PlatformDivider />}
                            {typeIcon && <TypeIcon>{typeIcon}</TypeIcon>}
                            <PlatformText>{type}</PlatformText>
                            {container && (
                                <Link to={entityRegistry.getEntityUrl(EntityType.Container, container?.urn)}>
                                    <PlatformDivider />
                                    <ContainerIcon
                                        style={{
                                            color: ANTD_GRAY[9],
                                        }}
                                    />
                                    <ContainerText>
                                        {entityRegistry.getDisplayName(EntityType.Container, container)}
                                    </ContainerText>
                                </Link>
                            )}
                            {entityCount && entityCount > 0 ? (
                                <>
                                    <PlatformDivider />
                                    <EntityCountText>{entityCount.toLocaleString()} entities</EntityCountText>
                                </>
                            ) : null}
                            {degree !== undefined && degree !== null && (
                                <span>
                                    <PlatformDivider />
                                    <Tooltip
                                        title={`This entity is a ${getNumberWithOrdinal(degree)} degree connection to ${
                                            entityData?.name || 'the source entity'
                                        }`}
                                    >
                                        <PlatformText>{getNumberWithOrdinal(degree)}</PlatformText>
                                    </Tooltip>
                                </span>
                            )}
                        </PlatformInfo>
                        <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                            {name || ' '}
                        </EntityTitle>
                    </Link>
                </TitleContainer>
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer limit={250}>{description}</NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {(domain || glossaryTerms || tags) && (
                    <TagContainer>
                        <TagTermGroup domain={domain} maxShow={3} />
                        {domain && glossaryTerms && <TagSeparator />}
                        <TagTermGroup uneditableGlossaryTerms={glossaryTerms} maxShow={1} />
                        {((glossaryTerms && tags) || (domain && tags)) && <TagSeparator />}
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
            </div>
        </PreviewContainer>
    );
}
