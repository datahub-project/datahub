import DataProcessInstanceRightColumn from '@app/preview/DataProcessInstanceRightColumn';
import React, { ReactNode, useState } from 'react';
import { Divider, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import {
    GlobalTags,
    Owner,
    GlossaryTerms,
    SearchInsight,
    Container,
    Dataset,
    ParentContainersResult,
    Maybe,
    CorpUser,
    Deprecation,
    Domain,
    EntityPath,
    DataProduct,
    Health,
    Entity,
} from '../../types.generated';
import TagTermGroup from '../shared/tags/TagTermGroup';
import { ANTD_GRAY } from '../entity/shared/constants';
import NoMarkdownViewer from '../entity/shared/components/styled/StripMarkdownText';
import { getNumberWithOrdinal } from '../entity/shared/utils';
import { useEntityData } from '../entity/shared/EntityContext';
import PlatformContentView from '../entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import useContentTruncation from '../shared/useContentTruncation';
import EntityCount from '../entity/shared/containers/profile/header/EntityCount';
import { ExpandedActorGroup } from '../entity/shared/components/styled/ExpandedActorGroup';
import { DeprecationPill } from '../entity/shared/components/styled/DeprecationPill';
import { PreviewType } from '../entity/Entity';
import ExternalUrlButton from '../entity/shared/ExternalUrlButton';
import EntityPaths from './EntityPaths/EntityPaths';
import { DataProductLink } from '../shared/tags/DataProductLink';
import { EntityHealth } from '../entity/shared/containers/profile/header/EntityHealth';
import SearchTextHighlighter from '../search/matches/SearchTextHighlighter';
import { getUniqueOwners } from './utils';
import StructuredPropertyBadge from '../entity/shared/containers/profile/header/StructuredPropertyBadge';
import { usePreviewData } from '../entity/shared/PreviewContext';

const PreviewContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const LeftColumn = styled.div<{ expandWidth: boolean }>`
    max-width: ${(props) => (props.expandWidth ? '100%' : '60%')};
`;

const RightColumn = styled.div`
    max-width: 40%;
    display: flex;
`;

const TitleContainer = styled.div`
    margin-bottom: 5px;
    line-height: 30px;

    .entityCount {
        margin-bottom: 2px;
    }
`;

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    display: block;
    &&&:hover {
        text-decoration: underline;
    }

    &&& {
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 600;
        vertical-align: middle;
    }
`;

const CardEntityTitle = styled(EntityTitle)`
    max-width: 350px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
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

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 3px;
    flex-wrap: wrap;
    margin-right: 8px;
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

const UserListContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: right;
    margin-right: 8px;
`;

const UserListDivider = styled(Divider)`
    padding: 4px;
    height: auto;
`;

const UserListTitle = styled(Typography.Text)`
    text-align: right;
    margin-bottom: 10px;
    padding-right: 12px;
`;

interface Props {
    name: string;
    urn: string;
    logoUrl?: string;
    logoComponent?: JSX.Element;
    url: string;
    description?: string;
    type?: string;
    typeIcon?: JSX.Element;
    platform?: string;
    platformInstanceId?: string;
    platforms?: Maybe<string | undefined>[];
    logoUrls?: Maybe<string | undefined>[];
    qualifier?: string | null;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    deprecation?: Deprecation | null;
    topUsers?: Array<CorpUser> | null;
    externalUrl?: string | null;
    entityTitleSuffix?: React.ReactNode;
    subHeader?: React.ReactNode;
    snippet?: React.ReactNode;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms;
    container?: Container;
    domain?: Domain | undefined | null;
    dataProduct?: DataProduct | undefined | null;
    entityCount?: number;
    displayAssetCount?: boolean;
    dataTestID?: string;
    titleSizePx?: number;
    onClick?: () => void;
    // this is provided by the impact analysis view. it is used to display
    // how the listed node is connected to the source node
    degree?: number;
    parentContainers?: ParentContainersResult | null;
    parentEntities?: Entity[] | null;
    previewType?: Maybe<PreviewType>;
    paths?: EntityPath[];
    health?: Health[];
    parentDataset?: Dataset;
    dataProcessInstanceProps?: {
        startTime?: number;
        duration?: number;
        status?: string;
    };
}

export default function DefaultPreviewCard({
    name,
    urn,
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
    topUsers,
    subHeader,
    snippet,
    insights,
    glossaryTerms,
    domain,
    dataProduct,
    container,
    deprecation,
    entityCount,
    displayAssetCount,
    titleSizePx,
    dataTestID,
    externalUrl,
    entityTitleSuffix,
    onClick,
    degree,
    parentContainers,
    parentEntities,
    platforms,
    logoUrls,
    previewType,
    paths,
    health,
    parentDataset,
    dataProcessInstanceProps,
}: Props) {
    // sometimes these lists will be rendered inside an entity container (for example, in the case of impact analysis)
    // in those cases, we may want to enrich the preview w/ context about the container entity
    const { entityData } = useEntityData();
    const previewData = usePreviewData();
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
    const [descriptionExpanded, setDescriptionExpanded] = useState(false);

    const { contentRef, isContentTruncated } = useContentTruncation(container);

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    const shouldShowRightColumn =
        (topUsers && topUsers.length > 0) ||
        (owners && owners.length > 0) ||
        dataProcessInstanceProps?.startTime ||
        dataProcessInstanceProps?.duration ||
        dataProcessInstanceProps?.status;
    const uniqueOwners = getUniqueOwners(owners);

    return (
        <PreviewContainer data-testid={dataTestID} onMouseDown={onPreventMouseDown}>
            <LeftColumn key="left-column" expandWidth={!shouldShowRightColumn}>
                <TitleContainer>
                    <PlatformContentView
                        platformName={platform}
                        platformLogoUrl={logoUrl}
                        platformNames={platforms}
                        platformLogoUrls={logoUrls}
                        entityLogoComponent={logoComponent}
                        instanceId={platformInstanceId}
                        typeIcon={typeIcon}
                        entityType={type}
                        parentContainers={parentContainers?.containers}
                        parentEntities={parentEntities}
                        parentContainersRef={contentRef}
                        areContainersTruncated={isContentTruncated}
                        parentDataset={parentDataset}
                    />
                    <EntityTitleContainer>
                        <Link to={url}>
                            {previewType === PreviewType.HOVER_CARD ? (
                                <CardEntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                                    {name || ' '}
                                </CardEntityTitle>
                            ) : (
                                <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                                    <SearchTextHighlighter field="name" text={name || ''} />
                                </EntityTitle>
                            )}
                        </Link>
                        {deprecation?.deprecated && (
                            <DeprecationPill deprecation={deprecation} urn="" showUndeprecate={false} />
                        )}
                        {health && health.length > 0 ? <EntityHealth baseUrl={url} health={health} /> : null}
                        <StructuredPropertyBadge structuredProperties={previewData?.structuredProperties} />
                        {externalUrl && (
                            <ExternalUrlButton
                                externalUrl={externalUrl}
                                platformName={platform}
                                entityUrn={urn}
                                entityType={type}
                            />
                        )}
                        {entityTitleSuffix}
                    </EntityTitleContainer>
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
                    <EntityCount entityCount={entityCount} displayAssetsText={displayAssetCount} />
                </TitleContainer>
                {paths && paths.length > 0 && <EntityPaths paths={paths} resultEntityUrn={urn || ''} />}
                {description && description.length > 0 && (
                    <DescriptionContainer>
                        <NoMarkdownViewer
                            limit={descriptionExpanded ? undefined : 250}
                            shouldWrap={previewType === PreviewType.HOVER_CARD}
                            readMore={
                                previewType === PreviewType.HOVER_CARD ? (
                                    <Typography.Link
                                        onClickCapture={(e) => {
                                            onPreventMouseDown(e);
                                            setDescriptionExpanded(!descriptionExpanded);
                                        }}
                                    >
                                        {descriptionExpanded ? 'Show Less' : 'Show More'}
                                    </Typography.Link>
                                ) : undefined
                            }
                            customRender={(text) => <SearchTextHighlighter field="description" text={text} />}
                        >
                            {description}
                        </NoMarkdownViewer>
                    </DescriptionContainer>
                )}
                {(dataProduct || domain || hasGlossaryTerms || hasTags) && (
                    <TagContainer>
                        {/* if there's a domain and dataProduct, show dataProduct */}
                        {dataProduct && <DataProductLink dataProduct={dataProduct} />}
                        {!dataProduct && domain && <TagTermGroup domain={domain} maxShow={3} />}
                        {(dataProduct || domain) && hasGlossaryTerms && <TagSeparator />}
                        {hasGlossaryTerms && <TagTermGroup uneditableGlossaryTerms={glossaryTerms} maxShow={3} />}
                        {((hasGlossaryTerms && hasTags) || ((dataProduct || domain) && hasTags)) && <TagSeparator />}
                        {hasTags && <TagTermGroup uneditableTags={tags} maxShow={3} />}
                    </TagContainer>
                )}
                {subHeader}
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
            </LeftColumn>
            {shouldShowRightColumn && (
                <RightColumn key="right-column">
                    <DataProcessInstanceRightColumn {...dataProcessInstanceProps} />
                    {topUsers && topUsers?.length > 0 && (
                        <>
                            <UserListContainer>
                                <UserListTitle strong>Top Users</UserListTitle>
                                <div>
                                    <ExpandedActorGroup actors={topUsers} max={2} />
                                </div>
                            </UserListContainer>
                        </>
                    )}
                    {(topUsers?.length || 0) > 0 && (uniqueOwners?.length || 0) > 0 && (
                        <UserListDivider type="vertical" />
                    )}
                    {uniqueOwners && uniqueOwners?.length > 0 && (
                        <UserListContainer>
                            <UserListTitle strong>Owners</UserListTitle>
                            <div>
                                <ExpandedActorGroup actors={uniqueOwners.map((owner) => owner.owner)} max={2} />
                            </div>
                        </UserListContainer>
                    )}
                </RightColumn>
            )}
        </PreviewContainer>
    );
}
