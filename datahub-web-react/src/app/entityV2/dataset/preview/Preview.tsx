import React from 'react';
import { useTheme } from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { DatasetStatsSummary as DatasetStatsSummaryView } from '@app/entityV2/dataset/shared/DatasetStatsSummary';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { DatasetLastUpdatedMs, summaryHasStats } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    BrowsePathV2,
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
    data,
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
    statsSummary,
    lastUpdatedMs,
    health,
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
    statsSummary?: DatasetStatsSummary | null;
    lastUpdatedMs?: DatasetLastUpdatedMs;
    health?: Health[] | null;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    tier?: PopularityTier;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
    browsePaths?: BrowsePathV2;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const hasStats = !!columnCount || summaryHasStats(statsSummary);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Dataset}
            type={subtype}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.Dataset, 14, IconStyleType.ACCENT)}
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
            parentEntities={parentContainers?.containers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            subHeader={
                hasStats && (
                    <DatasetStatsSummaryView
                        columnCount={columnCount}
                        rowCount={rowCount}
                        queryCountLast30Days={statsSummary?.queryCountLast30Days}
                        uniqueUserCountLast30Days={statsSummary?.uniqueUserCountLast30Days}
                        color={theme.colors.textSecondary}
                    />
                )
            }
            health={health || undefined}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            lastUpdatedMs={lastUpdatedMs}
            tier={tier}
            headerDropdownItems={headerDropdownItems}
            statsSummary={statsSummary}
            previewType={previewType}
            browsePaths={browsePaths}
        />
    );
};
