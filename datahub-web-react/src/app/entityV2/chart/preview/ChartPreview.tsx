import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType } from '@app/entityV2/Entity';
import { ChartStatsSummary as ChartStatsSummaryView } from '@app/entityV2/chart/shared/ChartStatsSummary';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { DashboardLastUpdatedMs, summaryHasStats } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    AccessLevel,
    BrowsePathV2,
    ChartStatsSummary,
    Container,
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
} from '@types';

export const ChartPreview = ({
    urn,
    data,
    name,
    description,
    platform,
    platformInstanceId,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
    dataProduct,
    container,
    insights,
    logoUrl,
    deprecation,
    statsSummary,
    lastUpdatedMs,
    createdMs,
    externalUrl,
    parentContainers,
    snippet,
    degree,
    paths,
    subType,
    isOutputPort,
    tier,
    headerDropdownItems,
    browsePaths,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    platform?: string;
    platformInstanceId?: string;
    name?: string;
    description?: string | null;
    access?: AccessLevel | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags;
    glossaryTerms?: GlossaryTerms | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    container?: Container | null;
    insights?: Array<SearchInsight> | null;
    logoUrl?: string | null;
    deprecation?: Deprecation | null;
    statsSummary?: ChartStatsSummary | null;
    lastUpdatedMs?: DashboardLastUpdatedMs;
    createdMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
    snippet?: React.ReactNode | null;
    degree?: number;
    paths?: EntityPath[];
    subType?: string | null;
    isOutputPort?: boolean;
    tier?: PopularityTier;
    headerDropdownItems?: Set<EntityMenuItems>;
    browsePaths?: BrowsePathV2 | undefined;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const hasStats = summaryHasStats(statsSummary);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Chart, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Chart}
            type={subType}
            typeIcon={entityRegistry.getIcon(EntityType.Chart, 14, IconStyleType.ACCENT)}
            logoUrl={logoUrl || ''}
            platform={platform}
            platformInstanceId={platformInstanceId}
            qualifier={access}
            tags={tags}
            owners={owners}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            dataProduct={dataProduct}
            container={container || undefined}
            insights={insights}
            parentEntities={parentContainers?.containers}
            deprecation={deprecation}
            externalUrl={externalUrl}
            snippet={snippet}
            topUsers={statsSummary?.topUsersLast30Days}
            subHeader={
                hasStats && (
                    <ChartStatsSummaryView
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
            browsePaths={browsePaths}
        />
    );
};
