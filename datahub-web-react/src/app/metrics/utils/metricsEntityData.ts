import { GenericEntityProperties } from '@app/entity/shared/types';
import { MetricsEntityData } from '@app/metrics/context/MetricsEntityContext';

import { EntityType } from '@types';

type MetricsProfileProperties = GenericEntityProperties & {
    semanticModel?: {
        urn: string;
        info?: {
            name?: string | null;
        } | null;
    } | null;
    parentMetrics?: Array<{
        urn: string;
    }> | null;
};

export function mapProfileToMetricsEntityData(
    entityData: GenericEntityProperties,
    entityType: EntityType,
): MetricsEntityData {
    const profile = entityData as MetricsProfileProperties;

    return {
        urn: profile.urn ?? '',
        entityType,
        semanticModel: profile.semanticModel
            ? {
                  urn: profile.semanticModel.urn,
                  name: profile.semanticModel.info?.name,
              }
            : null,
        parentMetrics: profile.parentMetrics?.map(({ urn }) => ({ urn })) ?? null,
        platform: profile.platform ?? null,
        domain: profile.domain?.domain ?? null,
    };
}
