import { GenericEntityProperties } from '@app/entity/shared/types';
import { mapProfileToMetricsEntityData } from '@app/metrics/utils/metricsEntityData';

import { DataPlatform, Domain, EntityType } from '@types';

const platform = {
    urn: 'urn:li:dataPlatform:snowflake',
    type: EntityType.DataPlatform,
} as DataPlatform;

const domain = {
    urn: 'urn:li:domain:finance',
    type: EntityType.Domain,
} as Domain;

describe('metrics entity data', () => {
    it('maps the profile fields needed to locate a metric in the sidebar', () => {
        const profile = {
            urn: 'urn:li:metric:revenue',
            platform,
            domain: { associatedUrn: domain.urn, domain },
            semanticModel: {
                urn: 'urn:li:semanticModel:finance',
                info: { name: 'Finance model' },
            },
            parentMetrics: [{ urn: 'urn:li:metric:parent' }],
        } as GenericEntityProperties;

        expect(mapProfileToMetricsEntityData(profile, EntityType.Metric)).toEqual({
            urn: profile.urn,
            entityType: EntityType.Metric,
            semanticModel: {
                urn: 'urn:li:semanticModel:finance',
                name: 'Finance model',
            },
            parentMetrics: [{ urn: 'urn:li:metric:parent' }],
            platform,
            domain,
        });
    });

    it('normalizes absent optional profile fields to null', () => {
        expect(mapProfileToMetricsEntityData({ urn: 'urn:li:semanticModel:empty' }, EntityType.SemanticModel)).toEqual({
            urn: 'urn:li:semanticModel:empty',
            entityType: EntityType.SemanticModel,
            semanticModel: null,
            parentMetrics: null,
            platform: null,
            domain: null,
        });
    });
});
