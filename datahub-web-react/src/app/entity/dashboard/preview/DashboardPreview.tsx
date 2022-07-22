import React from 'react';
import {
    AccessLevel,
    Domain,
    Container,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    SearchInsight,
    ParentContainersResult,
    Deprecation,
    DashboardStatsSummary,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';
import { DashboardStatsSummary as DashboardStatsSummaryView } from '../shared/DashboardStatsSummary';

export const DashboardPreview = ({
    urn,
    name,
    platformInstanceId,
    description,
    platform,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
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
}: {
    urn: string;
    platform: string;
    platformInstanceId?: string;
    name?: string;
    description?: string | null;
    access?: AccessLevel | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags;
    glossaryTerms?: GlossaryTerms | null;
    domain?: Domain | null;
    container?: Container | null;
    deprecation?: Deprecation | null;
    insights?: Array<SearchInsight> | null;
    logoUrl?: string | null;
    chartCount?: number | null;
    statsSummary?: DashboardStatsSummary | null;
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platform);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dashboard, urn)}
            name={name || ''}
            description={description || ''}
            type="Dashboard"
            typeIcon={entityRegistry.getIcon(EntityType.Dashboard, 14, IconStyleType.ACCENT)}
            logoUrl={logoUrl || ''}
            platformInstanceId={platformInstanceId}
            platform={capitalizedPlatform}
            qualifier={access}
            owners={owners}
            tags={tags}
            container={container || undefined}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            deprecation={deprecation}
            insights={insights}
            parentContainers={parentContainers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            subHeader={
                <DashboardStatsSummaryView
                    chartCount={chartCount}
                    viewCount={statsSummary?.viewCount}
                    uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                    lastUpdatedMs={lastUpdatedMs}
                    createdMs={createdMs}
                />
            }
        />
    );
};
