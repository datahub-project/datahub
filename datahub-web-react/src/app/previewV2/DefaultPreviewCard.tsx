import { PartitionOutlined } from '@ant-design/icons';
import AccountCircleOutlinedIcon from '@mui/icons-material/AccountCircleOutlined';
// import BookOutlinedIcon from '@mui/icons-material/BookOutlined';
import FindInPageOutlinedIcon from '@mui/icons-material/FindInPageOutlined';
// import CameraIcon from '@mui/icons-material/Camera';
// import FindInPageIcon from '@mui/icons-material/FindInPage';
import LaunchIcon from '@mui/icons-material/Launch';
import ReportProblemIcon from '@mui/icons-material/ReportProblem';
import SellOutlinedIcon from '@mui/icons-material/SellOutlined';
import { Tooltip, Typography } from 'antd';
import React, { ReactNode, useContext } from 'react';
import { Link } from 'react-router-dom';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import GlossaryTermV2Icon from '../../images/glossary_term_material_logo.svg?react';
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
import { EntityCapabilityType, PreviewType } from '../entityV2/Entity';
import { useEntityData } from '../entityV2/shared/EntityContext';
import { ANTD_GRAY, SEARCH_COLORS } from '../entityV2/shared/constants';
import { EntityHealth } from '../entityV2/shared/containers/profile/header/EntityHealth';
import PopularityIcon from '../entityV2/shared/containers/profile/sidebar/shared/popularity/PopularityIcon';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { getNumberWithOrdinal } from '../entityV2/shared/utils';
import { useMatchedFieldsForList } from '../search/context/SearchResultContext';
import SearchTextHighlighter from '../searchV2/matches/SearchTextHighlighter';
import LastUpdated from '../shared/LastUpdated';
import MatchesContext, { PreviewSection } from '../shared/MatchesContext';
import { isHealthy, isUnhealthy } from '../shared/health/healthUtils';
import useContentTruncation from '../shared/useContentTruncation';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import CardActionCircle from './CardActionCircle';
import ColoredBackgroundPlatformIconGroup from './ColoredBackgroundPlatformIconGroup';
import SearchCardBrowsePath from './SearchCardBrowsePath';
import SearchPill from './SearchPill';
import { entityHasCapability } from './utils';

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

const EntityTitleContainer = styled.div<{ $alignTop?: boolean }>`
    display: flex;
    align-items: center;
    ${(props) => props.$alignTop && 'margin-top: -22px;'}
    width: 75%;
`;

const StyledLink = styled(Link)`
    display: block;
    width: 100%;
`;

const LineageContainer = styled.div<{ highlighted?: boolean }>`
    display: flex;
    & svg {
        font-size: 12px;
        color: ${({ highlighted }) => (highlighted ? '#3F54D1' : '#b0a2c2')} !important;
    }
`;

const HealthContainer = styled.div<{ healthy?: boolean; unhealthy?: boolean }>`
    display: flex;
    & svg {
        font-size: 12px;
        color: ${({ healthy, unhealthy }) => {
            if (healthy) {
                return '#00b341';
            }
            if (unhealthy) {
                return '#d0021b';
            }
            return '#b0a2c2';
        }} !important;
    }
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
        display: block;
        &&&:hover {
            text-decoration: underline;
        }
    &&& {
        margin-right 8px;
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 700;
        vertical-align: middle;
    }
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: Manrope;
    font-size: 13px;
    color: ${SEARCH_COLORS.LINK_BLUE};
    width: 75%;
    height: 100%;
`;

const CardEntityTitle = styled(EntityTitle)<{ $previewType?: Maybe<PreviewType> }>`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: ${(props) => (props.$previewType === PreviewType.HOVER_CARD ? `100px` : '250px')};
`;

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

const PillsSection = styled.div`
    gap: 5px;
    display: flex;
    flex-direction: row;
    align-items: left;
    height: 30px;
`;

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

const CardStatusGroup = styled.div`
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

const EntityLink = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;

    .ant-btn-link {
        display: flex;
        align-items: center;
        color: #56668E;
        height: 100%;

        :hover {
            color: #533FD1;
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
    const { setExpandedSection, expandedSection } = useContext(MatchesContext);
    const unhealthy = health && isUnhealthy(health);
    const healthy = health && isHealthy(health);
    const supportedCapabilities = entityRegistry.getSupportedEntityCapabilities(entityType);
    const showLineageBadge = entityHasCapability(supportedCapabilities, EntityCapabilityType.LINEAGE);
    const showHealthBadge = entityHasCapability(supportedCapabilities, EntityCapabilityType.HEALTH);
    const showOwnersBadge = entityHasCapability(supportedCapabilities, EntityCapabilityType.OWNERS);
    const showGlossaryTermsBadge = entityHasCapability(supportedCapabilities, EntityCapabilityType.GLOSSARY_TERMS);
    const showTagsBadge = entityHasCapability(supportedCapabilities, EntityCapabilityType.TAGS);

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
    const groupedMatches = useMatchedFieldsForList('fieldLabels');
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
                    {(showLineageBadge || showHealthBadge) && (
                        <CardStatusGroup>
                            {showLineageBadge && (
                                <>
                                    <Tooltip
                                        title={`${upstreamTotal} upstreams, ${downstreamTotal} downstreams`}
                                        showArrow={false}
                                    >
                                        <LineageContainer
                                            highlighted={(upstreamTotal || 0) + (downstreamTotal || 0) > 0}
                                            onClick={() =>
                                                history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`)
                                            }
                                        >
                                            <PartitionOutlined />
                                        </LineageContainer>
                                    </Tooltip>
                                </>
                            )}
                            {showHealthBadge && (
                                <HealthContainer healthy={healthy} unhealthy={unhealthy}>
                                    {health && health.length > 0 && (healthy || unhealthy) ? (
                                        <EntityHealth baseUrl={url} health={health} />
                                    ) : (
                                        <Tooltip title="No health information available" showArrow={false}>
                                            <ReportProblemIcon />
                                        </Tooltip>
                                    )}
                                </HealthContainer>
                            )}
                            {!!lastUpdatedMs && (
                                <LastUpdated
                                    noLabel
                                    time={lastUpdatedMs}
                                    typeName={finalType}
                                    platformName={platform}
                                    platformLogoUrl={logoUrl}
                                />
                            )}
                        </CardStatusGroup>
                    )}
                    <ActionsSection>
                        {externalUrl && (
                            <a href={externalUrl || undefined} target="blank">
                                <CardActionCircle enabled={!!externalUrl} icon={<LaunchIcon />} />
                            </a>
                        )}

                        {/* <CardActionCircle enabled={deprecation?.deprecated} icon={<ErrorOutlineIcon />} /> */}
                        {/* <CardActionCircle enabled icon={<ReportGmailerrorredIcon />} /> */}
                    </ActionsSection>
                </ActionsAndStatusSection>
            </RowContainer>
            <RowContainer>
                {previewType === PreviewType.HOVER_CARD ? (
                    <EntityTitleContainer $alignTop={!isIconPresent}>
                        <StyledLink to={url}>
                            <CardEntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                                {name || ' '}
                            </CardEntityTitle>
                        </StyledLink>
                    </EntityTitleContainer>
                ) : (
                    <EntityTitleContainer $alignTop={!isIconPresent}>
                        <StyledLink to={url}>
                            <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                                <SearchTextHighlighter field="name" text={name || ''} />
                            </EntityTitle>
                        </StyledLink>
                    </EntityTitleContainer>
                )}
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
                {(showOwnersBadge || showGlossaryTermsBadge || showTagsBadge || groupedMatches.length > 0) && (
                    <PillsSection>
                        {showGlossaryTermsBadge && (
                            <SearchPill
                                icon={<GlossaryTermV2Icon />}
                                count={glossaryTerms?.terms?.length || 0}
                                enabled={!!glossaryTerms?.terms?.length}
                                active={expandedSection === PreviewSection.GLOSSARY_TERMS}
                                label=""
                                countLabel="term"
                                onClick={(e) => {
                                    if (!glossaryTerms?.terms?.length) return;
                                    setExpandedSection(
                                        expandedSection === PreviewSection.GLOSSARY_TERMS
                                            ? undefined
                                            : PreviewSection.GLOSSARY_TERMS,
                                    );
                                    e.preventDefault();
                                    e.stopPropagation();
                                }}
                            />
                        )}
                        {showTagsBadge && (
                            <SearchPill
                                icon={<SellOutlinedIcon />}
                                count={tags?.tags?.length || 0}
                                enabled={!!tags?.tags?.length}
                                active={expandedSection === PreviewSection.TAGS}
                                label=""
                                countLabel="tag"
                                onClick={(e) => {
                                    if (!tags?.tags?.length) return;
                                    setExpandedSection(
                                        expandedSection === PreviewSection.TAGS ? undefined : PreviewSection.TAGS,
                                    );
                                    e.preventDefault();
                                    e.stopPropagation();
                                }}
                            />
                        )}
                        {showOwnersBadge && (
                            <SearchPill
                                icon={<AccountCircleOutlinedIcon />}
                                count={owners?.length || 0}
                                enabled={!!owners?.length}
                                active={expandedSection === PreviewSection.OWNERS}
                                label=""
                                countLabel="owner"
                                onClick={(e) => {
                                    if (!owners?.length) return;
                                    setExpandedSection(
                                        expandedSection === PreviewSection.OWNERS ? undefined : PreviewSection.OWNERS,
                                    );
                                    e.preventDefault();
                                    e.stopPropagation();
                                }}
                            />
                        )}

                        {groupedMatches.length > 0 && (
                            <SearchPill
                                icon={<FindInPageOutlinedIcon />}
                                count={groupedMatches?.length || 0}
                                enabled
                                active={expandedSection === PreviewSection.MATCHES}
                                label=""
                                countLabel="match"
                                onClick={(e) => {
                                    if (!groupedMatches?.length) return;
                                    setExpandedSection(
                                        expandedSection === PreviewSection.MATCHES ? undefined : PreviewSection.MATCHES,
                                    );
                                    e.preventDefault();
                                    e.stopPropagation();
                                }}
                            />
                        )}
                    </PillsSection>
                )}
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
