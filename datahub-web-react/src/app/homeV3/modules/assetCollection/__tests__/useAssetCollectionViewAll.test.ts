import { describe, expect, it } from 'vitest';

import { convertLogicalPredicateToViewAllParams } from '@app/homeV3/modules/assetCollection/useAssetCollectionViewAll';
import { ENTITY_SUB_TYPE_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { EntityType, FilterOperator } from '@types';

const getTypeFromGraphName = (name: string): EntityType | undefined =>
    (({ dataProduct: EntityType.DataProduct, dataset: EntityType.Dataset }) as Record<string, EntityType>)[name];

const predicate = (operator: LogicalOperatorType, operands: LogicalPredicate['operands']): LogicalPredicate => ({
    type: 'logical',
    operator,
    operands,
});

describe('convertLogicalPredicateToViewAllParams', () => {
    it('converts a flat AND of entity type and tags', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:golden'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toEqual({
            unionType: UnionType.AND,
            filters: [
                { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATA_PRODUCT'], condition: FilterOperator.Equal },
                { field: 'tags', values: ['urn:li:tag:golden'], condition: FilterOperator.Equal },
            ],
        });
    });

    it('converts a flat OR with unionType OR', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.OR, [
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
                { type: 'property', property: 'owners', operator: 'equals', values: ['urn:li:corpuser:b'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result?.unionType).toEqual(UnionType.OR);
        expect(result?.filters).toHaveLength(2);
    });

    it('skips blank builder rows but keeps valid leaves', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property' },
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataset'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result?.filters).toEqual([
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASET'], condition: FilterOperator.Equal },
        ]);
    });

    it('returns null for nested groups', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.OR, [
                predicate(LogicalOperatorType.AND, [
                    { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
                ]),
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for NOT predicates', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.NOT, [
                { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for exists operator', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [{ type: 'property', property: 'tags', operator: 'exists' }]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null when an exists leaf appears alongside convertible leaves (must not be silently dropped)', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] },
                { type: 'property', property: 'tags', operator: 'exists' },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for unknown operators', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: 'tags', operator: 'soundsLike', values: ['x'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null for unmappable entity type values', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property', property: '_entityType', operator: 'equals', values: ['notARealType'] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });

    it('returns null when no usable leaves remain', () => {
        const result = convertLogicalPredicateToViewAllParams(
            predicate(LogicalOperatorType.AND, [
                { type: 'property' },
                { type: 'property', property: 'tags', operator: 'equals', values: [] },
            ]),
            getTypeFromGraphName,
        );
        expect(result).toBeNull();
    });
});
