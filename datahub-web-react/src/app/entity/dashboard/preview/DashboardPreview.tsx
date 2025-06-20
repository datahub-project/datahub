import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { DashboardStatsSummary as DashboardStatsSummaryView } from '@app/entity/dashboard/shared/DashboardStatsSummary';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    Health,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '@types';

export const DashboardPreview = ({
    urn,
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
    health,
}: {
    urn: string;
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
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
    snippet?: React.ReactNode | null;
    degree?: number;
    paths?: EntityPath[];
    health?: Health[] | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dashboard, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Dashboard'}
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
            parentContainers={parentContainers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            snippet={snippet}
            subHeader={
                <DashboardStatsSummaryView
                    chartCount={chartCount}
                    viewCount={statsSummary?.viewCount}
                    uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                    lastUpdatedMs={lastUpdatedMs}
                    createdMs={createdMs}
                />
            }
            degree={degree}
            paths={paths}
            health={health || undefined}
        />
    );
};
