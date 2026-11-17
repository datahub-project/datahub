import { getSidebarStructuredPropertiesOrFilters } from '@app/entityV2/shared/sidebarSection/utils';
import { EntityType } from '@src/types.generated';
import { getTestEntityRegistry } from '@src/utils/test-utils/TestPageContainer';

describe('getSidebarStructuredPropertiesOrFilters', () => {
    const entityRegistry = getTestEntityRegistry();

    it('should return correct filters for Dataset entity type in non-schema sidebar', () => {
        const result = getSidebarStructuredPropertiesOrFilters(false, entityRegistry, EntityType.Dataset);

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual({
            and: [
                {
                    field: 'entityTypes',
                    values: ['urn:li:entityType:datahub.dataset'],
                },
                {
                    field: 'isHidden',
                    values: ['true'],
                    negated: true,
                },
                {
                    field: 'showInAssetSummary',
                    values: ['true'],
                },
            ],
        });
    });

    it('should return correct filters for SchemaField entity type in schema sidebar', () => {
        const result = getSidebarStructuredPropertiesOrFilters(true, entityRegistry, EntityType.SchemaField);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual({
            and: [
                {
                    field: 'entityTypes',
                    values: ['urn:li:entityType:datahub.schemaField'],
                },
                {
                    field: 'isHidden',
                    values: ['true'],
                    negated: true,
                },
                {
                    field: 'showInAssetSummary',
                    values: ['true'],
                },
            ],
        });
        expect(result[1]).toEqual({
            and: [
                {
                    field: 'entityTypes',
                    values: ['urn:li:entityType:datahub.schemaField'],
                },
                {
                    field: 'isHidden',
                    values: ['true'],
                    negated: true,
                },
                {
                    field: 'showInColumnsTable',
                    values: ['true'],
                },
            ],
        });
    });
});
