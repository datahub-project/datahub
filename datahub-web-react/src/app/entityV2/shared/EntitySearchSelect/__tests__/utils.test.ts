import { getEntityDisplayName, getEntityTypeLabel } from '@app/entityV2/shared/EntitySearchSelect/utils';

import { Entity, EntityType } from '@types';

describe('EntitySearchSelect utils', () => {
    describe('getEntityTypeLabel', () => {
        const mockTranslate = (key: string, defaultValue: string) => {
            const translations: Record<string, string> = {
                customOwnershipType: 'Propriétaire personnalisé',
                ingestionSource: "Source d'ingestion",
            };
            return translations[key] || defaultValue;
        };

        it('should return translated label for CustomOwnershipType', () => {
            const result = getEntityTypeLabel(EntityType.CustomOwnershipType, mockTranslate);
            expect(result).toBe('Propriétaire personnalisé');
        });

        it('should return default label for CustomOwnershipType when translation missing', () => {
            const noTranslate = (key: string, defaultValue: string) => defaultValue;
            const result = getEntityTypeLabel(EntityType.CustomOwnershipType, noTranslate);
            expect(result).toBe('Custom Ownership Type');
        });

        it('should return translated label for IngestionSource', () => {
            const result = getEntityTypeLabel(EntityType.IngestionSource, mockTranslate);
            expect(result).toBe("Source d'ingestion");
        });

        it('should return default label for IngestionSource when translation missing', () => {
            const noTranslate = (key: string, defaultValue: string) => defaultValue;
            const result = getEntityTypeLabel(EntityType.IngestionSource, noTranslate);
            expect(result).toBe('Ingestion Source');
        });

        it('should return raw entity type for unknown types', () => {
            const result = getEntityTypeLabel(EntityType.Dataset, () => 'SHOULD_NOT_USE');
            expect(result).toBe(EntityType.Dataset);
        });
    });

    describe('getEntityDisplayName', () => {
        const mockEntityRegistry = {
            getDisplayName: (type: EntityType, entity: Entity) => {
                if (type === EntityType.Dataset) {
                    return (entity as any).name || entity.urn;
                }
                return '';
            },
        } as any;

        it('should extract name from CustomOwnershipType info field', () => {
            const entity: Entity = {
                urn: 'urn:li:ownershipType:test',
                type: EntityType.CustomOwnershipType,
                info: { name: 'Test Owner Type' },
            } as any;

            const result = getEntityDisplayName(entity, mockEntityRegistry);
            expect(result).toBe('Test Owner Type');
        });

        it('should fall back to URN when CustomOwnershipType has no info.name', () => {
            const entity: Entity = {
                urn: 'urn:li:ownershipType:test',
                type: EntityType.CustomOwnershipType,
                info: {},
            } as any;

            const result = getEntityDisplayName(entity, mockEntityRegistry);
            expect(result).toBe('urn:li:ownershipType:test');
        });

        it('should extract name from IngestionSource directly', () => {
            const entity: Entity = {
                urn: 'urn:li:ingestionSource:test',
                type: EntityType.IngestionSource,
                name: 'Test Source',
            } as any;

            const result = getEntityDisplayName(entity, mockEntityRegistry);
            expect(result).toBe('Test Source');
        });

        it('should fall back to URN when IngestionSource has no name', () => {
            const entity: Entity = {
                urn: 'urn:li:ingestionSource:test',
                type: EntityType.IngestionSource,
            } as any;

            const result = getEntityDisplayName(entity, mockEntityRegistry);
            expect(result).toBe('urn:li:ingestionSource:test');
        });

        it('should use registry for registered entity types', () => {
            const entity: Entity = {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
                name: 'Test Dataset',
            } as any;

            const result = getEntityDisplayName(entity, mockEntityRegistry);
            expect(result).toBe('Test Dataset');
        });
    });
});
