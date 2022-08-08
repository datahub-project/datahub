import React from 'react';
import {
    EntityType,
    FabricType,
    Owner,
    GlobalTags,
    GlossaryTerms,
    SearchInsight,
    Domain,
    Container,
    ParentContainersResult,
    Maybe,
    Deprecation,
    DatasetStatsSummary,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';
import { DatasetStatsSummary as DatasetStatsSummaryView } from '../shared/DatasetStatsSummary';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    platformNames,
    platformLogos,
    platformInstanceId,
    owners,
    globalTags,
    domain,
    deprecation,
    snippet,
    insights,
    glossaryTerms,
    subtype,
    externalUrl,
    container,
    parentContainers,
    rowCount,
    statsSummary,
    lastUpdatedMs,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    platformNames?: (Maybe<string> | undefined)[];
    platformLogos?: (Maybe<string> | undefined)[];
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    deprecation?: Deprecation | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms | null;
    subtype?: string | null;
    externalUrl?: string | null;
    container?: Container | null;
    parentContainers?: ParentContainersResult | null;
    rowCount?: number | null;
    statsSummary?: DatasetStatsSummary | null;
    lastUpdatedMs?: number | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetterOnly(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}
            name={name || ''}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Dataset'}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT)}
            platform={capitalPlatformName}
            platforms={platformNames}
            logoUrls={platformLogos}
            platformInstanceId={platformInstanceId}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            container={container || undefined}
            deprecation={deprecation}
            snippet={snippet}
            glossaryTerms={glossaryTerms || undefined}
            insights={insights}
            parentContainers={parentContainers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            subHeader={
                <DatasetStatsSummaryView
                    rowCount={rowCount}
                    queryCountLast30Days={statsSummary?.queryCountLast30Days}
                    uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                    lastUpdatedMs={lastUpdatedMs}
                />
            }
        />
    );
};
