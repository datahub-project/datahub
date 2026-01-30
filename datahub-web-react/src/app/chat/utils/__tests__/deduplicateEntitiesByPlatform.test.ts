import { describe, expect, it } from 'vitest';

import { deduplicateEntitiesByPlatform } from '@app/chat/utils/deduplicateEntitiesByPlatform';

import { EntityType } from '@types';

describe('deduplicateEntitiesByPlatform', () => {
    it('returns empty array for empty input', () => {
        expect(deduplicateEntitiesByPlatform([])).toEqual([]);
    });

    it('returns single entity unchanged', () => {
        const entity = {
            urn: 'urn:li:dataset:1',
            type: EntityType.Dataset,
            platform: { urn: 'urn:li:dataPlatform:snowflake' },
        };
        const result = deduplicateEntitiesByPlatform([entity as any]);
        expect(result).toHaveLength(1);
        expect(result[0].urn).toBe('urn:li:dataset:1');
    });

    it('deduplicates entities with same platform', () => {
        const entities = [
            {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
            {
                urn: 'urn:li:dataset:2',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
            {
                urn: 'urn:li:dataset:3',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:bigquery' },
            },
        ];
        const result = deduplicateEntitiesByPlatform(entities as any[]);
        expect(result).toHaveLength(2);
        expect(result[0].urn).toBe('urn:li:dataset:1');
        expect(result[1].urn).toBe('urn:li:dataset:3');
    });

    it('keeps first entity for each platform', () => {
        const entities = [
            {
                urn: 'urn:li:dataset:first-snowflake',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
            {
                urn: 'urn:li:dataset:second-snowflake',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
        ];
        const result = deduplicateEntitiesByPlatform(entities as any[]);
        expect(result).toHaveLength(1);
        expect(result[0].urn).toBe('urn:li:dataset:first-snowflake');
    });

    it('uses entity URN as fallback when no platform', () => {
        const entities = [
            {
                urn: 'urn:li:corpuser:user1',
                type: EntityType.CorpUser,
            },
            {
                urn: 'urn:li:corpuser:user2',
                type: EntityType.CorpUser,
            },
        ];
        const result = deduplicateEntitiesByPlatform(entities as any[]);
        expect(result).toHaveLength(2);
    });

    it('handles mixed entities with and without platforms', () => {
        const entities = [
            {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
            {
                urn: 'urn:li:corpuser:user1',
                type: EntityType.CorpUser,
            },
            {
                urn: 'urn:li:dataset:2',
                type: EntityType.Dataset,
                platform: { urn: 'urn:li:dataPlatform:snowflake' },
            },
        ];
        const result = deduplicateEntitiesByPlatform(entities as any[]);
        expect(result).toHaveLength(2);
        expect(result[0].urn).toBe('urn:li:dataset:1');
        expect(result[1].urn).toBe('urn:li:corpuser:user1');
    });
});
