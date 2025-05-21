import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { ChartStatsSummary as ChartStatsSummaryView } from '@app/entity/chart/shared/ChartStatsSummary';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    Health,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '@types';

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
    health,
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
    lastUpdatedMs?: number | null;
    createdMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
    snippet?: React.ReactNode | null;
    degree?: number;
    paths?: EntityPath[];
    subType?: string | null;
    health?: Health[] | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Chart, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subType) || 'Chart'}
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
            subHeader={
                <ChartStatsSummaryView
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
