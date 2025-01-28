import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import {
    AccessLevel,
    Container,
    DashboardStatsSummary,
    DataProduct,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    ParentContainersResult,
    SearchInsight,
    BrowsePathV2,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import { PopularityTier } from '../../shared/containers/profile/sidebar/shared/utils';
import { summaryHasStats, DashboardLastUpdatedMs } from '../../shared/utils';
import { DashboardStatsSummary as DashboardStatsSummaryView } from '../shared/DashboardStatsSummary';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const DashboardPreview = ({
    urn,
    data,
    platform,
    platformInstanceId,
    name,
    subtype,
    description,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
    dataProduct,
    container,
    insights,
    logoUrl,
    chartCount,
    statsSummary,
    lastUpdatedMs,
    createdMs,
    externalUrl,
    parentContainers,
    deprecation,
    snippet,
    degree,
    paths,
    isOutputPort,
    tier,
    headerDropdownItems,
    previewType,
    browsePaths,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    platform?: string;
    platformInstanceId?: string;
    name?: string;
    subtype?: string | null;
    description?: string | null;
    access?: AccessLevel | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags;
    glossaryTerms?: GlossaryTerms | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    container?: Container | null;
    deprecation?: Deprecation | null;
    insights?: Array<SearchInsight> | null;
    logoUrl?: string | null;
    chartCount?: number | null;
    statsSummary?: DashboardStatsSummary | null;
    lastUpdatedMs?: DashboardLastUpdatedMs;
    createdMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
    snippet?: React.ReactNode | null;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    tier?: PopularityTier;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
    browsePaths?: BrowsePathV2;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const hasStats = summaryHasStats(statsSummary);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dashboard, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Dashboard}
            type={subtype}
            typeIcon={entityRegistry.getIcon(EntityType.Dashboard, 14, IconStyleType.ACCENT)}
            logoUrl={logoUrl || ''}
            platformInstanceId={platformInstanceId}
            platform={platform}
            qualifier={access}
            owners={owners}
            tags={tags}
            container={container || undefined}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            dataProduct={dataProduct}
            deprecation={deprecation}
            insights={insights}
            parentEntities={parentContainers?.containers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            snippet={snippet}
            subHeader={
                hasStats && (
                    <DashboardStatsSummaryView
                        chartCount={chartCount}
                        viewCount={statsSummary?.viewCount}
                        viewCountLast30Days={statsSummary?.viewCountLast30Days}
                        uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                        createdMs={createdMs}
                    />
                )
            }
            degree={degree}
            paths={paths}
            lastUpdatedMs={lastUpdatedMs}
            isOutputPort={isOutputPort}
            tier={tier}
            headerDropdownItems={headerDropdownItems}
            statsSummary={statsSummary}
            previewType={previewType}
            browsePaths={browsePaths}
        />
    );
};
