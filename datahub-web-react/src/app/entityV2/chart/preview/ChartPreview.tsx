import React from 'react';
import {
    AccessLevel,
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
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { PopularityTier } from '../../shared/containers/profile/sidebar/shared/utils';
import { summaryHasStats } from '../../shared/utils';
import { ChartStatsSummary as ChartStatsSummaryView } from '../shared/ChartStatsSummary';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const ChartPreview = ({
    urn,
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
}: {
    urn: string;
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
    lastUpdatedMs?: { property?: string, lastUpdatedMs?: number };
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
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const hasStats = summaryHasStats(statsSummary);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Chart, urn)}
            name={name || ''}
            urn={urn}
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
            parentContainers={parentContainers}
            deprecation={deprecation}
            externalUrl={externalUrl}
            snippet={snippet}
            topUsers={statsSummary?.topUsersLast30Days}
            subHeader={
                hasStats && (
                    <ChartStatsSummaryView
                        viewCount={statsSummary?.viewCount}
                        viewCountLast30Days={statsSummary?.viewCountLast30Days}
                        viewCountPercentileLast30Days={statsSummary?.viewCountPercentileLast30Days}
                        uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                        uniqueUserPercentileLast30Days={statsSummary?.uniqueUserPercentileLast30Days}
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
        />
    );
};
