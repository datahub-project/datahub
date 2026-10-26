import { describe, expect, it } from 'vitest';

import {
    DEFAULT_CARDINALITY_PILL_COLOR,
    getCardinalityLabelKey,
    getCardinalityPillColor,
    getRelationshipRowKey,
    indexDatasetsByAliasOrName,
} from '@app/entityV2/summary/modules/semanticModelRelationships/utils';

import { Dataset, EntityType, ErModelRelationshipCardinality, SemanticModelRelationship } from '@types';

describe('semanticModelRelationships utils', () => {
    describe('indexDatasetsByAliasOrName', () => {
        it('indexes by alias when present, otherwise by name', () => {
            const aliased = {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                name: 'db.schema.opportunities',
                semanticModelProperties: { alias: 'OPPORTUNITIES' },
            } as Dataset;
            const plain = {
                urn: 'urn:li:dataset:2',
                type: EntityType.Dataset,
                name: 'targets',
            } as Dataset;

            const map = indexDatasetsByAliasOrName([aliased, plain]);
            expect(map.get('OPPORTUNITIES')?.urn).toBe('urn:li:dataset:1');
            expect(map.get('targets')?.urn).toBe('urn:li:dataset:2');
            expect(map.get('db.schema.opportunities')).toBeUndefined();
        });
    });

    describe('getCardinalityPillColor', () => {
        it('returns mapped colors and gray default', () => {
            expect(getCardinalityPillColor(ErModelRelationshipCardinality.OneN)).toBe('green');
            expect(getCardinalityPillColor(null)).toBe(DEFAULT_CARDINALITY_PILL_COLOR);
            expect(getCardinalityPillColor(undefined)).toBe(DEFAULT_CARDINALITY_PILL_COLOR);
        });
    });

    describe('getCardinalityLabelKey', () => {
        it('returns i18n keys for each cardinality', () => {
            expect(getCardinalityLabelKey(ErModelRelationshipCardinality.NOne)).toBe(
                'semanticModelRelationships.cardinality.manyOne',
            );
        });
    });

    describe('getRelationshipRowKey', () => {
        it('includes index for stable keys even when name is present', () => {
            const named = { name: 'opp_to_acct', from: 'OPPORTUNITIES', to: 'ACCOUNTS' } as SemanticModelRelationship;
            const unnamed = { from: 'OPPORTUNITIES', to: 'ACCOUNTS' } as SemanticModelRelationship;
            expect(getRelationshipRowKey(named, 0)).toBe('opp_to_acct-0');
            expect(getRelationshipRowKey(named, 1)).toBe('opp_to_acct-1');
            expect(getRelationshipRowKey(unnamed, 2)).toBe('OPPORTUNITIES-ACCOUNTS-2');
        });
    });
});
