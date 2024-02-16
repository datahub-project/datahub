// import BookOutlinedIcon from '@mui/icons-material/BookOutlined';
// import CameraIcon from '@mui/icons-material/Camera';
// import FindInPageIcon from '@mui/icons-material/FindInPage';
import LaunchIcon from '@mui/icons-material/Launch';
import { Tooltip, Typography } from 'antd';
import React, { ReactNode } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import {
    Container,
    CorpUser,
    DataProduct,
    Deprecation,
    Domain,
    Entity,
    EntityPath,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Health,
    Maybe,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '../../types.generated';
import { PreviewType } from '../entityV2/Entity';
import { useEntityData } from '../entityV2/shared/EntityContext';
import { ANTD_GRAY } from '../entityV2/shared/constants';
import PopularityIcon from '../entityV2/shared/containers/profile/sidebar/shared/popularity/PopularityIcon';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { getNumberWithOrdinal } from '../entityV2/shared/utils';
import useContentTruncation from '../shared/useContentTruncation';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import CardActionCircle from './CardActionCircle';
import ColoredBackgroundPlatformIconGroup from './ColoredBackgroundPlatformIconGroup';
import SearchCardBrowsePath from './SearchCardBrowsePath';
import Pills from './Pills';
import StatusBadges from './StatusBadges';
import EntityHeader from './EntityHeader';
import EntityExternalLink from '../entityV2/shared/links/EntityExternalLink';

const PreviewContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    justify-content: space-between;
    align-items: left;

    .entityCount {
        margin-bottom: 2px;
    }
`;

interface RowContainerProps {
    hidden?: boolean;
    alignment?: 'flex-start' | 'center' | 'flex-end' | 'self-start';
}

const RowContainer = styled.div<RowContainerProps>`
    align-items: ${(props) => props.alignment || 'center'};
    display: ${(props) => (props.hidden ? 'none' : 'flex')};
    flex-direction: row;
    justify-content: space-between;
`;

const RowContainerJustifyStart = styled(RowContainer)`
    justify-content: start;
`;

// const HorizontalDivider = styled.hr`
//     margin: 10px 0;
//     opacity: 0.1;
//     width: 100%;
// `;

const SummaryContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    margin-bottom: 12px;
    margin-top: 2px;
`;

// const SummaryTitle = styled.span`
//     font-size: 10px;
//     opacity: 0.5;
//     margin-bottom: 4px;
// `;

const StatsContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    overflow-x: hidden;
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 21px;
    vertical-align: text-top;
`;

const DegreeText = styled.span`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    margin-top: 12px;
    width: fit-content;
`;

// const InsightsContainer = styled.div`
//     display: flex;
//     margin-top: 7px;
// `;

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
    urn: string;
    logoUrl?: string;
    logoComponent?: JSX.Element;
    url: string;
    entityType: EntityType;
    type?: string | null;
    typeIcon?: JSX.Element;
    platform?: string;
    platformInstanceId?: string;
    platforms?: Maybe<string | undefined>[];
    logoUrls?: Maybe<string | undefined>[];
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    deprecation?: Deprecation | null;
    topUsers?: Array<CorpUser> | null;
    entityTitleSuffix?: React.ReactNode;
    subHeader?: React.ReactNode;
    snippet?: React.ReactNode;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms;
    container?: Container;
    domain?: Domain | undefined | null;
    dataProduct?: DataProduct | undefined | null;
    entityCount?: number;
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
    lastUpdatedMs?: number | null;
    // eslint-disable-next-line react/no-unused-prop-types
    description?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    qualifier?: string | null;
    // eslint-disable-next-line react/no-unused-prop-types
    externalUrl?: string | null;
    tier?: PopularityTier;
    isOutputPort?: boolean;
    upstreamTotal?: number;
    downstreamTotal?: number;
    entityIcon?: JSX.Element;
}

const ActionsSection = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
`;

const ActionsAndStatusSection = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
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

export default function DefaultPreviewCard({
    name,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    urn,
    logoUrl,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    logoComponent,
    url,
    entityType,
    type,
    typeIcon,
    platform,
    platformInstanceId,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    tags,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    owners,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    topUsers,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    glossaryTerms,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    paths,
    subHeader,
    snippet,
    insights,
    domain,
    dataProduct,
    container,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    deprecation,
    entityCount,
    titleSizePx,
    dataTestID,
    entityTitleSuffix,
    onClick,
    degree,
    parentContainers,
    parentEntities,
    platforms,
    logoUrls,
    previewType,
    health,
    lastUpdatedMs,
    externalUrl,
    tier,
    isOutputPort,
    upstreamTotal,
    downstreamTotal,
    entityIcon,
}: Props) {
    const entityRegistry = useEntityRegistryV2();
    const history = useHistory();
    const supportedCapabilities = entityRegistry.getSupportedEntityCapabilities(entityType);

    // sometimes these lists will be rendered inside an entity container (for example, in the case of impact analysis)
    // in those cases, we may want to enrich the preview w/ context about the container entity
    const { entityData } = useEntityData();
    const insightViews: Array<ReactNode> =
        insights?.map((insight) => (
            <>
                <InsightIconContainer>{insight.icon}</InsightIconContainer>
                <InsightsText>{insight.text}</InsightsText>
            </>
        )) || [];

    if (snippet) {
        insightViews.push(snippet);
    }
    const { contentRef } = useContentTruncation(container);

    const onPreventMouseDown = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    // TODO: Replace with something less hacky
    const finalType = type || entityRegistry.getEntityName(entityType);
    const hasPlatformIcons =
        platform || logoUrl || (platforms && platforms.length) || (logoUrls && logoUrls.length) || isOutputPort;
    const isIconPresent = !!hasPlatformIcons || !!entityIcon;

    return (
        <PreviewContainer data-testid={dataTestID} onMouseDown={onPreventMouseDown}>
            <RowContainer alignment="self-start">
                {isIconPresent ? (
                    <ColoredBackgroundPlatformIconGroup
                        platformName={platform}
                        platformLogoUrl={logoUrl}
                        platformNames={platforms}
                        platformLogoUrls={logoUrls}
                        isOutputPort={isOutputPort}
                        icon={entityIcon}
                    />
                ) : (
                    <div />
                )}
                <ActionsAndStatusSection>
                    <StatusBadges
                        upstreamTotal={upstreamTotal}
                        downstreamTotal={downstreamTotal}
                        health={health}
                        entityType={entityType}
                        urn={urn}
                        url={url}
                        history={history}
                        entityRegistry={entityRegistry}
                        entityCapabilities={supportedCapabilities}
                        lastUpdatedMs={lastUpdatedMs}
                        finalType={finalType}
                        platform={platform}
                        logoUrl={logoUrl}
                    />
                    <ActionsSection>
                        {externalUrl && (
                            <EntityExternalLink url={externalUrl}>
                                <CardActionCircle enabled={!!externalUrl} icon={<LaunchIcon />} />
                            </EntityExternalLink>
                        )}
                        {/* <CardActionCircle enabled={deprecation?.deprecated} icon={<ErrorOutlineIcon />} /> */}
                        {/* <CardActionCircle enabled icon={<ReportGmailerrorredIcon />} /> */}
                    </ActionsSection>
                </ActionsAndStatusSection>
            </RowContainer>
            <RowContainer>
                <EntityHeader
                    name={name}
                    onClick={onClick}
                    previewType={previewType}
                    titleSizePx={titleSizePx}
                    url={url}
                />
            </RowContainer>
            <RowContainer style={{ marginTop: 8 }}>
                <SearchCardBrowsePath
                    instanceId={platformInstanceId}
                    typeIcon={typeIcon}
                    type={finalType}
                    entityType={entityType}
                    parentContainers={parentContainers?.containers}
                    parentEntities={parentEntities}
                    parentContainersRef={contentRef}
                    areContainersTruncated={false}
                    entityTitleWidth={previewType === PreviewType.HOVER_CARD ? 100 : 200}
                    previewType={previewType}
                />
            </RowContainer>
            {!!(subHeader || dataProduct || domain) && (
                <>
                    <RowContainerJustifyStart>
                        {subHeader && (
                            <SummaryContainer>
                                <StatsContainer>{subHeader}</StatsContainer>
                            </SummaryContainer>
                        )}
                        {/* {(!!dataProduct || !!domain) && (
                                <GovernanceSection>
                                    {subHeader && <VerticalDivider />}
                                    <GovernanceContainer>
                                        {domain && (
                                            <GovernanceItem>
                                                <DomainLink
                                                    domain={domain}
                                                    tagStyle={{ border: 'none', fontSize: 12, color: '#328980', margin: 0 }}
                                                />
                                            </GovernanceItem>
                                        )}
                                        {dataProduct && (
                                            <GovernanceItem>
                                                <DataProductLink
                                                    dataProduct={dataProduct}
                                                    tagStyle={{ border: 'none', fontSize: 12, color: '#328980', margin: 0 }}
                                                />
                                            </GovernanceItem>
                                        )}
                                    </GovernanceContainer>
                                </GovernanceSection>
                            )} */}
                    </RowContainerJustifyStart>
                </>
            )}
            <RowContainer
                style={{
                    borderTop: `1px solid ${ANTD_GRAY[4]}`,
                    paddingTop: 12,
                    marginBottom: -14,
                }}
            >
                <Pills
                    glossaryTerms={glossaryTerms}
                    tags={tags}
                    owners={owners}
                    entityCapabilities={supportedCapabilities}
                />
                <EntityLink>{entityTitleSuffix}</EntityLink>
                {tier !== undefined && (
                    <div>
                        <PopularityIcon tier={tier} />
                    </div>
                )}
            </RowContainer>
            {/* {!!(insights?.length || groupedMatches.length) && isMatchExpanded && (
                    <>
                        <HorizontalDivider />
                        <SummaryTitle>Matches</SummaryTitle>
                        <InsightsContainer>
                            {insightViews.map((insightView, index) => (
                                <span>
                                    {insightView}
                                    {index < insightViews.length - 1 && <PlatformDivider />}
                                </span>
                            ))}
                        </InsightsContainer>
                    </>
                )} */}
            {degree !== undefined && degree !== null && (
                <Tooltip
                    title={`This entity is a ${getNumberWithOrdinal(degree)} degree connection to ${
                        entityData?.name || 'the source entity'
                    }`}
                >
                    <DegreeText>{getNumberWithOrdinal(degree)}</DegreeText>
                </Tooltip>
            )}
            {!!degree && entityCount && <PlatformDivider />}
        </PreviewContainer>
    );
}
