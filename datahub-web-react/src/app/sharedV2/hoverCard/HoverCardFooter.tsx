import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import SidebarPopularityHeaderSection, {
    hasPopularityStats,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarPopularityHeaderSection';
import {
    DashboardLastUpdatedMs,
    DatasetLastUpdatedMs,
    getDashboardLastUpdatedMs,
    getDatasetLastUpdatedMs,
} from '@app/entityV2/shared/utils';
import Freshness from '@app/previewV2/Freshness';
import LineageBadge from '@app/previewV2/LineageBadge';
import QueryStat from '@app/previewV2/QueryStat';
import { useHideLineageInSearchCards } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import {
    Chart,
    ChartStatsSummary,
    Dashboard,
    DashboardStatsSummary,
    Dataset,
    DatasetStatsSummary,
    Entity,
    EntityType,
    Operation,
} from '@types';

const Footer = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    padding-top: 8px;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

const Divider = styled.div`
    width: 1px;
    height: 16px;
    background: ${(props) => props.theme.colors.border};
`;

type Props = {
    entity: Entity;
    properties: GenericEntityProperties | null;
};

/**
 * The percentile fields that drive the popularity bars are not part of the open-source schema;
 * DataHub Cloud's fragments add them to the same `statsSummary` objects. Typing them optionally
 * here lets the footer light them up wherever they exist without a separate code path.
 */
type StatsSummaryWithPopularity = (DatasetStatsSummary | ChartStatsSummary | DashboardStatsSummary) & {
    queryCountPercentileLast30Days?: number | null;
    uniqueUserPercentileLast30Days?: number | null;
    viewCountPercentileLast30Days?: number | null;
};

function getStatsSummary(entity: Entity): StatsSummaryWithPopularity | null {
    switch (entity.type) {
        case EntityType.Dataset:
            return ((entity as Dataset).statsSummary as StatsSummaryWithPopularity | null | undefined) ?? null;
        case EntityType.Chart:
            return ((entity as Chart).statsSummary as StatsSummaryWithPopularity | null | undefined) ?? null;
        case EntityType.Dashboard:
            return ((entity as Dashboard).statsSummary as StatsSummaryWithPopularity | null | undefined) ?? null;
        default:
            return null;
    }
}

/**
 * `lastOperation` is an aliased `operations(limit: 1)` selection that only the dataset search
 * fragment asks for, so it lives on the raw entity rather than on the generic properties.
 */
function getLastUpdatedMs(entity: Entity): DatasetLastUpdatedMs | DashboardLastUpdatedMs | undefined {
    switch (entity.type) {
        case EntityType.Dataset: {
            const dataset = entity as Dataset & { lastOperation?: Operation[] | null };
            return getDatasetLastUpdatedMs(dataset.properties, dataset.lastOperation);
        }
        case EntityType.Dashboard:
        case EntityType.Chart:
            return getDashboardLastUpdatedMs((entity as Dashboard | Chart).properties);
        default:
            return undefined;
    }
}

/**
 * The usage row from the bottom of the search card: query volume, lineage, and freshness. Every
 * item is data-gated, and the whole footer is omitted when none of them have anything to show,
 * so tags, terms, users, and domains never grow an empty strip.
 */
export default function HoverCardFooter({ entity, properties }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const hideLineage = useHideLineageInSearchCards();

    const statsSummary = getStatsSummary(entity);
    const queryCount =
        entity.type === EntityType.Dataset
            ? ((statsSummary as DatasetStatsSummary | null)?.queryCountLast30Days ?? 0)
            : 0;
    const showPopularity = hasPopularityStats(
        entity.type,
        statsSummary?.queryCountPercentileLast30Days,
        statsSummary?.uniqueUserPercentileLast30Days,
        statsSummary?.viewCountPercentileLast30Days,
    );

    const lastUpdatedMs = getLastUpdatedMs(entity);

    const upstreamTotal = (properties?.upstream?.total || 0) - (properties?.upstream?.filtered || 0);
    const downstreamTotal = (properties?.downstream?.total || 0) - (properties?.downstream?.filtered || 0);
    // Only claim lineage when the hover fragment actually fetched it; a bare `0` would render the
    // greyed-out "no lineage" icon for entities we simply didn't ask about.
    const hasLineageData = !!properties?.upstream || !!properties?.downstream;
    const showLineage =
        !hideLineage &&
        hasLineageData &&
        entityRegistry.getSupportedEntityCapabilities(entity.type).has(EntityCapabilityType.LINEAGE);

    const items: { key: string; node: React.ReactNode }[] = [];
    if (queryCount > 0) {
        items.push({ key: 'queries', node: <QueryStat queryCountLast30Days={queryCount} /> });
    }
    if (showLineage) {
        items.push({
            key: 'lineage',
            node: (
                <LineageBadge
                    upstreamTotal={upstreamTotal}
                    downstreamTotal={downstreamTotal}
                    entityRegistry={entityRegistry}
                    entityType={entity.type}
                    urn={entity.urn}
                />
            ),
        });
    }
    if (lastUpdatedMs?.lastUpdatedMs) {
        items.push({
            key: 'freshness',
            node: (
                <Freshness time={lastUpdatedMs.lastUpdatedMs} timeProperty={lastUpdatedMs.property} showDate={false} />
            ),
        });
    }
    if (showPopularity) {
        items.push({
            key: 'popularity',
            node: <SidebarPopularityHeaderSection statsSummary={statsSummary} entityType={entity.type} size="small" />,
        });
    }

    if (items.length === 0) return null;

    return (
        <Footer>
            {items.map(({ key, node }, index) => (
                <React.Fragment key={key}>
                    {index > 0 && <Divider />}
                    {node}
                </React.Fragment>
            ))}
        </Footer>
    );
}
