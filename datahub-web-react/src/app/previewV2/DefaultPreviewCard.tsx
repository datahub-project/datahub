import { CloseOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { ReactNode } from 'react';
import styled from 'styled-components';

import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuActions, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import MoreOptionsMenuAction from '@app/entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import { usePreviewData } from '@app/entityV2/shared/PreviewContext';
import { useSearchCardContext } from '@app/entityV2/shared/SearchCardContext';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { GlossaryPreviewCardDecoration } from '@app/entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { DashboardLastUpdatedMs, DatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import ColoredBackgroundPlatformIconGroup from '@app/previewV2/ColoredBackgroundPlatformIconGroup';
import { CompactView } from '@app/previewV2/CompactView';
import ContextPath from '@app/previewV2/ContextPath';
import DefaultPreviewCardFooter from '@app/previewV2/DefaultPreviewCardFooter';
import EntityHeader from '@app/previewV2/EntityHeader';
import { ActionsAndStatusSection } from '@app/previewV2/shared';
import { useRemoveDataProductAssets, useRemoveDomainAssets, useRemoveGlossaryTermAssets } from '@app/previewV2/utils';
import { useSearchContext } from '@app/search/context/SearchContext';
import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import DataProcessInstanceInfo from '@src/app/preview/DataProcessInstanceInfo';

import {
    BrowsePathV2,
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
    SearchInsight,
} from '@types';

const TransparentButton = styled(Button)`
    color: ${(p) => p.theme.styles['primary-color']};
    font-size: 12px;
    box-shadow: none;
    border: none;
    padding: 0px 10px;
    display: none;

    &&& span {
        font-size: 12px;
    }

    &:hover {
        display: flex;
        align-items: center;
        opacity: 0.9;
        color: ${(p) => p.theme.styles['primary-color']};
    }
`;

const PreviewContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: auto;
    width: 100%;
    justify-content: space-between;
    align-items: start;
    .entityCount {
        margin-bottom: 2px;
    }

    &:hover ${TransparentButton} {
        display: inline-block;
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

const Documentation = styled.div`
    margin-top: 8px;
    max-height: 300px;
    overflow-y: auto;
`;

const ENTITY_TYPES_WITH_DESCRIPTION_PREVIEW = new Set([
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.DataProduct,
    EntityType.Domain,
    EntityType.Tag,
]);

interface Props {
    name: string;
    urn: string;
    data: GenericEntityProperties | null;
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
    parentEntities?: Entity[] | null;
    previewType?: Maybe<PreviewType>;
    paths?: EntityPath[];
    health?: Health[];
    lastUpdatedMs?: DatasetLastUpdatedMs | DashboardLastUpdatedMs;
    description?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    qualifier?: string | null;
    // eslint-disable-next-line react/no-unused-prop-types
    externalUrl?: string | null;
    tier?: PopularityTier;
    isOutputPort?: boolean;
    entityIcon?: JSX.Element;
    headerDropdownItems?: Set<EntityMenuItems>;
    statsSummary?: any;
    actions?: EntityMenuActions;
    browsePaths?: BrowsePathV2 | undefined;
}

export default function DefaultPreviewCard({
    name,
    urn,
    data,
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
    parentEntities,
    platforms,
    logoUrls,
    previewType,
    health,
    lastUpdatedMs,
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
    const hasPlatformIcons = logoUrl || (logoUrls && logoUrls.length) || isOutputPort;
    const isIconPresent = !!hasPlatformIcons || !!entityIcon;

    const { isFullViewCard } = useSearchContext();

    const { removeRelationship, removeButtonText } = useRemoveRelationship(entityType);

    const lastRunEvent = data?.lastRunEvent;
    const shouldShowDPIinfo =
        lastRunEvent?.timestampMillis || lastRunEvent?.durationMillis || lastRunEvent?.result?.resultType;
    const entityHeader = (
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
            previewData={previewData}
        />
    );

    return (
        <PreviewContainer data-testid={dataTestID ?? `preview-${urn}`}>
            {(entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm) && (
                <GlossaryPreviewCardDecoration urn={urn} entityData={previewData} displayProperties={undefined} />
            )}
            {isFullViewCard || previewType === PreviewType.HOVER_CARD ? (
                <>
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
                            entityHeader
                        )}
                        <ActionsAndStatusSection>
                            {removeButtonText && (
                                <TransparentButton size="small" onClick={removeRelationship}>
                                    <CloseOutlined size={5} /> {removeButtonText}
                                </TransparentButton>
                            )}
                            <ViewInPlatform urn={urn} data={data} />
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
                        </ActionsAndStatusSection>
                    </RowContainer>
                    {isIconPresent && <RowContainer>{entityHeader}</RowContainer>}
                    <RowContainer style={{ marginTop: 8 }}>
                        <ContextPath
                            type={finalType}
                            entityType={entityType}
                            instanceId={platformInstanceId}
                            typeIcon={typeIcon}
                            browsePaths={browsePaths}
                            parentEntities={parentEntities}
                            entityTitleWidth={previewType === PreviewType.HOVER_CARD ? 150 : 200}
                            previewType={previewType}
                            contentRef={contentRef}
                        />
                    </RowContainer>
                    {(previewType === PreviewType.HOVER_CARD ||
                        ENTITY_TYPES_WITH_DESCRIPTION_PREVIEW.has(entityType)) &&
                    description ? (
                        <Documentation>
                            <CompactMarkdownViewer content={description} />
                        </Documentation>
                    ) : null}
                    {shouldShowDPIinfo && (
                        <RowContainer style={{ marginTop: 8, justifyContent: 'flex-end' }}>
                            <DataProcessInstanceInfo {...lastRunEvent} />
                        </RowContainer>
                    )}
                </>
            ) : (
                <CompactView
                    data={data}
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
                    headerDropdownItems={headerDropdownItems}
                    previewType={previewType}
                    urn={urn}
                    entityType={entityType}
                    platformInstanceId={platformInstanceId}
                    typeIcon={typeIcon}
                    finalType={finalType}
                    parentEntities={parentEntities}
                    contentRef={contentRef}
                    browsePaths={browsePaths}
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

function useRemoveRelationship(entityType: EntityType) {
    const { setShouldRefetchEmbeddedListSearch } = useEntityContext();
    const { showRemovalFromList } = useSearchCardContext();
    const { removeDomain } = useRemoveDomainAssets(setShouldRefetchEmbeddedListSearch);
    const { removeTerm } = useRemoveGlossaryTermAssets(setShouldRefetchEmbeddedListSearch);
    const { removeDataProduct } = useRemoveDataProductAssets(setShouldRefetchEmbeddedListSearch);

    const previewData = usePreviewData();
    const entityData = useEntityData();
    const pageEntityType = entityData.entityType;

    if (pageEntityType === EntityType.Domain) {
        return {
            removeRelationship: () => removeDomain(previewData?.urn),
            removeButtonText:
                showRemovalFromList && entityType !== EntityType.DataProduct ? 'Remove from Domain' : null,
        };
    }
    if (pageEntityType === EntityType.GlossaryTerm) {
        return {
            removeRelationship: () => removeTerm(previewData, entityData.urn),
            removeButtonText: showRemovalFromList ? 'Remove Glossary Term' : null,
        };
    }
    if (pageEntityType === EntityType.DataProduct) {
        return {
            removeRelationship: () => removeDataProduct(previewData?.urn),
            removeButtonText: showRemovalFromList ? 'Remove from Data Product' : null,
        };
    }
    return {
        removeRelationship: () => {},
        removeButtonText: null,
    };
}
