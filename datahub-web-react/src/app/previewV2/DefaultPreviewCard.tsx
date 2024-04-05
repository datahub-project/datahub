// import BookOutlinedIcon from '@mui/icons-material/BookOutlined';
// import CameraIcon from '@mui/icons-material/Camera';
// import FindInPageIcon from '@mui/icons-material/FindInPage';
import LaunchIcon from '@mui/icons-material/Launch';
import { Typography } from 'antd';
import React, { ReactNode } from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
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
import { ANTD_GRAY } from '../entityV2/shared/constants';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { usePreviewData } from '../entityV2/shared/PreviewContext';
import useContentTruncation from '../shared/useContentTruncation';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import CardActionCircle from './CardActionCircle';
import ColoredBackgroundPlatformIconGroup from './ColoredBackgroundPlatformIconGroup';
import SearchCardBrowsePath from './SearchCardBrowsePath';
import EntityHeader from './EntityHeader';
import EntityExternalLink from '../entityV2/shared/links/EntityExternalLink';
import { EntityMenuItems } from '../entityV2/shared/EntityDropdown/EntityMenuActions';
import MoreOptionsMenuAction from '../entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import DefaultPreviewCardFooter from './DefaultPreviewCardFooter';

const PreviewContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    justify-content: space-between;
    align-items: start;

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
    width: 100%;
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
    headerDropdownItems?: Set<EntityMenuItems>;
    statsSummary?: any;
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

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
`;

export default function DefaultPreviewCard({
    name, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    urn,
    logoUrl, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    logoComponent,
    url,
    entityType,
    type,
    typeIcon,
    platform,
    platformInstanceId, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    tags, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    owners, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    topUsers, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    glossaryTerms, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    paths, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    subHeader,
    snippet,
    insights, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    domain, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    dataProduct,
    container, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    deprecation, // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
    previewType, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    health, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    lastUpdatedMs,
    externalUrl,
    tier,
    isOutputPort, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    upstreamTotal, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    downstreamTotal,
    entityIcon,
    headerDropdownItems,
    statsSummary,
}: Props) {
    const entityRegistry = useEntityRegistryV2();
    const history = useHistory();
    const supportedCapabilities = entityRegistry.getSupportedEntityCapabilities(entityType);

    // sometimes these lists will be rendered inside an entity container (for example, in the case of impact analysis)
    // in those cases, we may want to enrich the preview w/ context about the container entity
    const previewData = usePreviewData();
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

    // TODO: Replace with something less hacky
    const finalType = type || entityRegistry.getEntityName(entityType);
    const hasPlatformIcons =
        platform || logoUrl || (platforms && platforms.length) || (logoUrls && logoUrls.length) || isOutputPort;
    const isIconPresent = !!hasPlatformIcons || !!entityIcon;

    return (
        <PreviewContainer data-testid={dataTestID}>
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
                    <ActionsSection>
                        {externalUrl && (
                            <EntityExternalLink url={externalUrl}>
                                <CardActionCircle enabled={!!externalUrl} icon={<LaunchIcon />} />
                            </EntityExternalLink>
                        )}

                        {headerDropdownItems && previewType !== PreviewType.HOVER_CARD && (
                            <MoreOptionsMenuAction
                                menuItems={headerDropdownItems}
                                urn={urn}
                                entityType={entityType}
                                entityData={previewData}
                                triggerType={['click']}
                            />
                        )}
                    </ActionsSection>
                </ActionsAndStatusSection>
            </RowContainer>
            <RowContainer>
                <HeaderContainer>
                    <EntityHeader
                        name={name}
                        onClick={onClick}
                        previewType={previewType}
                        titleSizePx={titleSizePx}
                        url={url}
                        urn={urn}
                        deprecation={deprecation}
                        health={health}
                        degree={degree}
                        connectionName={previewData?.name}
                    />
                </HeaderContainer>
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
                    entityTitleWidth={previewType === PreviewType.HOVER_CARD ? 150 : 200}
                    previewType={previewType}
                />
            </RowContainer>

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
            <DefaultPreviewCardFooter
                glossaryTerms={glossaryTerms}
                tags={tags}
                owners={owners}
                entityCapabilities={supportedCapabilities}
                tier={tier}
                previewType={previewType}
                entityTitleSuffix={entityTitleSuffix}
                upstreamTotal={upstreamTotal}
                downstreamTotal={downstreamTotal}
                entityType={entityType}
                urn={urn}
                history={history}
                entityRegistry={entityRegistry}
                lastUpdatedMs={lastUpdatedMs}
                statsSummary={statsSummary}
                paths={paths}
            />
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
        </PreviewContainer>
    );
}
