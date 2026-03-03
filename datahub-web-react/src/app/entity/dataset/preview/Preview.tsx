import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { DatasetStatsSummary as DatasetStatsSummaryView } from '@app/entity/dataset/shared/DatasetStatsSummary';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    Container,
    DataProduct,
    DatasetStatsSummary,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    FabricType,
    GlobalTags,
    GlossaryTerms,
    Health,
    Maybe,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '@types';

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
    dataProduct,
    deprecation,
    snippet,
    insights,
    glossaryTerms,
    subtype,
    externalUrl,
    container,
    parentContainers,
    rowCount,
    columnCount,
    sizeInBytes,
    statsSummary,
    lastUpdatedMs,
    health,
    degree,
    paths,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName?: string;
    platformLogo?: string | null;
    platformNames?: (Maybe<string> | undefined)[];
    platformLogos?: (Maybe<string> | undefined)[];
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
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
    columnCount?: number | null;
    sizeInBytes?: number | null;
    statsSummary?: DatasetStatsSummary | null;
    lastUpdatedMs?: number | null;
    health?: Health[] | null;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Dataset'}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT)}
            platform={platformName}
            platforms={platformNames}
            logoUrls={platformLogos}
            platformInstanceId={platformInstanceId}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            dataProduct={dataProduct}
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
                    columnCount={columnCount}
                    sizeInBytes={sizeInBytes}
                    lastUpdatedMs={lastUpdatedMs}
                />
            }
            health={health || undefined}
            degree={degree}
            paths={paths}
        />
    );
};
