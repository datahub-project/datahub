import { SubType } from '@app/entityV2/shared/components/subtypes';
import { getPlatformUrnFromEntityUrn } from '@app/entityV2/shared/utils';
import { DBT_URN, SQLMESH_URN } from '@app/ingestV2/source/builder/constants';

import { Dataset, EntityType, FeatureFlagsConfig } from '@types';

/**
 * Transformation tools whose datasets (models and sources) are siblings of the warehouse tables they
 * build or read. Lineage draws their datasets as transformations between tables, lineage search walks
 * through them rather than counting them as hops, and each tool's feature flag can fold its sources into
 * their warehouse sibling.
 */
const TRANSFORMATION_PLATFORMS: { platformUrn: string; hideSourcesFlag: keyof FeatureFlagsConfig }[] = [
    { platformUrn: DBT_URN, hideSourcesFlag: 'hideDbtSourceInLineage' },
    { platformUrn: SQLMESH_URN, hideSourcesFlag: 'hideSqlmeshSourceInLineage' },
];

export const TRANSFORMATION_PLATFORM_URNS = TRANSFORMATION_PLATFORMS.map(({ platformUrn }) => platformUrn);

export function isTransformationPlatform(node: { urn?: string; type: string }): boolean {
    return (
        (node.type === EntityType.Dataset || node.type === EntityType.SchemaField) &&
        !!node.urn &&
        TRANSFORMATION_PLATFORM_URNS.includes(getPlatformUrnFromEntityUrn(node.urn) ?? '')
    );
}

/** A warehouse table that a transformation tool reads but doesn't build, typed 'Source' by each tool. */
export function isSourceSubtype(subtype?: string | null): boolean {
    return subtype === SubType.DbtSource;
}

/** Whether lineage draws this source as its warehouse sibling, per its tool's feature flag. */
export function isSourceMergedIntoSibling(dataset?: Dataset | null, flags?: FeatureFlagsConfig): boolean {
    // Lineage query must include platform and typeNames on dataset and its sibling
    return TRANSFORMATION_PLATFORMS.some(
        ({ platformUrn, hideSourcesFlag }) =>
            !!flags?.[hideSourcesFlag] &&
            dataset?.platform?.urn === platformUrn &&
            !!dataset?.subTypes?.typeNames?.some(isSourceSubtype),
    );
}
