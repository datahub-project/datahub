import LaunchIcon from '@mui/icons-material/Launch';
import { Typography } from 'antd';
import React, { ReactNode } from 'react';
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
    BrowsePathV2,
} from '../../types.generated';
import { EntityMenuActions, PreviewType } from '../entityV2/Entity';
import { ANTD_GRAY, REDESIGN_COLORS } from '../entityV2/shared/constants';
import { PopularityTier } from '../entityV2/shared/containers/profile/sidebar/shared/utils';
import { usePreviewData } from '../entityV2/shared/PreviewContext';
import useContentTruncation from '../shared/useContentTruncation';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import ColoredBackgroundPlatformIconGroup from './ColoredBackgroundPlatformIconGroup';
import SearchCardBrowsePath from './SearchCardBrowsePath';
import StaticSearchCardBrowsePath from './StaticSearchCardBrowsePath';
import EntityHeader from './EntityHeader';
import { EntityMenuItems } from '../entityV2/shared/EntityDropdown/EntityMenuActions';
import MoreOptionsMenuAction from '../entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import DefaultPreviewCardFooter from './DefaultPreviewCardFooter';
import { GlossaryPreviewCardDecoration } from '../entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';
import { useSearchContext } from '../search/context/SearchContext';
import { CompactView } from './CompactView';

import { DatasetLastUpdatedMs, DashboardLastUpdatedMs } from '../entityV2/shared/utils';
import StyledExternalLink from '../entityV2/shared/links/StyledExternalLink';

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
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    description?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    qualifier?: string | null;
    externalUrl?: string | null;
    tier?: PopularityTier;
    isOutputPort?: boolean;
    entityIcon?: JSX.Element;
    headerDropdownItems?: Set<EntityMenuItems>;
    statsSummary?: any;
    actions?: EntityMenuActions;
    browsePaths?: BrowsePathV2 | undefined;
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
    width: 100%;
`;

const Documentation = styled.div`
    width: 90%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.SUB_TEXT};
    margin-top: 8px;
`;

const LeftContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
`;

export default function DefaultPreviewCard({
    name,
    urn,
    logoUrl, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    logoComponent,
    url,
    entityType,
    type,
    typeIcon,
    platform,
    platformInstanceId,
    tags,
    owners, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    topUsers,
    glossaryTerms,
    paths, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    subHeader,
    snippet,
    insights, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    domain, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    dataProduct,
    container,
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
    previewType,
    health,
    lastUpdatedMs,
    externalUrl,
    tier,
    isOutputPort,
    entityIcon,
    headerDropdownItems,
    statsSummary,
    actions,
    browsePaths,
    description,
}: Props) {
    const entityRegistry = useEntityRegistryV2();
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

    // Determine if entity has parent containers for rendering SearchBrowsePath or StaticSearchBrowsePath
    const hasParentContainers =
        (parentContainers && parentContainers.count > 0) || (parentEntities && parentEntities.length > 0);

    const { isFullViewCard } = useSearchContext();

    return (
        <PreviewContainer data-testid={dataTestID}>
            {(entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm) && (
                <GlossaryPreviewCardDecoration urn={urn} entityData={previewData} displayProperties={undefined} />
            )}
            {isFullViewCard ? (
                <>
                    <RowContainer alignment="self-start">
                        <LeftContainer>
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
                        </LeftContainer>

                        <ActionsAndStatusSection>
                            <ActionsSection>
                                {headerDropdownItems && previewType !== PreviewType.HOVER_CARD && (
                                    <MoreOptionsMenuAction
                                        menuItems={headerDropdownItems}
                                        urn={urn}
                                        entityType={entityType}
                                        entityData={previewData}
                                        triggerType={['click']}
                                        actions={actions}
                                    />
                                )}
                                {externalUrl && (
                                    <StyledExternalLink url={externalUrl}>
                                        <LaunchIcon
                                            style={{
                                                fontSize: '16px',
                                            }}
                                        />
                                        View in {platform}
                                    </StyledExternalLink>
                                )}
                            </ActionsSection>
                        </ActionsAndStatusSection>
                    </RowContainer>

                    {entityType === EntityType.GlossaryTerm && (
                        <RowContainer>
                            <Documentation>{description}</Documentation>
                        </RowContainer>
                    )}
                    <RowContainer style={{ marginTop: 8 }}>
                        {hasParentContainers && (
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
                        )}
                        {!hasParentContainers && (
                            <StaticSearchCardBrowsePath
                                entityType={entityType}
                                browsePaths={browsePaths}
                                type={finalType}
                                parentEntity={previewData?.parent}
                            />
                        )}
                    </RowContainer>
                </>
            ) : (
                <CompactView
                    name={name}
                    onClick={onClick}
                    titleSizePx={titleSizePx}
                    url={url}
                    degree={degree}
                    deprecation={deprecation}
                    actions={actions}
                    health={health}
                    previewData={previewData}
                    isIconPresent={isIconPresent}
                    platform={platform}
                    logoUrl={logoUrl}
                    platforms={platforms}
                    logoUrls={logoUrls}
                    isOutputPort={isOutputPort}
                    entityIcon={entityIcon}
                    externalUrl={externalUrl}
                    headerDropdownItems={headerDropdownItems}
                    previewType={previewType}
                    urn={urn}
                    entityType={entityType}
                    hasParentContainers={hasParentContainers}
                    platformInstanceId={platformInstanceId}
                    typeIcon={typeIcon}
                    finalType={finalType}
                    parentEntities={parentEntities}
                    parentContainers={parentContainers}
                    contentRef={contentRef}
                    browsePaths={browsePaths}
                    parentEntity={previewData?.parent}
                />
            )}
            <DefaultPreviewCardFooter
                glossaryTerms={glossaryTerms}
                tags={tags}
                owners={owners}
                entityCapabilities={supportedCapabilities}
                tier={tier}
                previewType={previewType}
                entityTitleSuffix={entityTitleSuffix}
                entityType={entityType}
                urn={urn}
                entityRegistry={entityRegistry}
                lastUpdatedMs={lastUpdatedMs}
                statsSummary={statsSummary}
                paths={paths}
                isFullViewCard={isFullViewCard}
            />
        </PreviewContainer>
    );
}
