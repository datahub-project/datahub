import { describe, expect, it } from 'vitest';

import {
    getDimensionGroups,
    getDimensionPillKind,
    isCalculatedDimension,
    isDefaultAliasQualifiedField,
} from '@app/entityV2/summary/modules/semanticModelDimensions/utils';

import { Dataset, EntityType, SchemaField, SemanticFieldType } from '@types';

function buildField(overrides: Partial<SchemaField> = {}): SchemaField {
    return {
        fieldPath: 'opportunity_id',
        ...overrides,
    } as SchemaField;
}

describe('semanticModelDimensions utils', () => {
    describe('isDefaultAliasQualifiedField', () => {
        it('matches bare and alias-qualified field paths case-insensitively', () => {
            expect(isDefaultAliasQualifiedField('opportunity_id', 'opportunity_id')).toBe(true);
            expect(isDefaultAliasQualifiedField('OPPORTUNITIES.opportunity_id', 'opportunity_id')).toBe(true);
            expect(isDefaultAliasQualifiedField('OPPORTUNITIES.OPPORTUNITY_ID', 'opportunity_id')).toBe(true);
        });

        it('rejects expressions that only suffix-match the field path', () => {
            expect(isDefaultAliasQualifiedField('1 + opportunities.opportunity_id', 'opportunity_id')).toBe(false);
        });

        it('rejects calculated expressions', () => {
            expect(isDefaultAliasQualifiedField('datediff(day, created_date, close_date)', 'close_date')).toBe(false);
            expect(isDefaultAliasQualifiedField('other_field', 'opportunity_id')).toBe(false);
        });
    });

    describe('isCalculatedDimension', () => {
        it('returns false for default alias-qualified expression', () => {
            const field = buildField({
                schemaFieldEntity: {
                    semanticFieldAnnotation: {
                        expression: {
                            dialects: [{ expression: 'OPPORTUNITIES.opportunity_id' }],
                        },
                    },
                },
            } as Partial<SchemaField>);
            expect(isCalculatedDimension(field)).toBe(false);
        });

        it('returns true for non-default expressions', () => {
            const field = buildField({
                fieldPath: 'close_date',
                schemaFieldEntity: {
                    semanticFieldAnnotation: {
                        expression: {
                            dialects: [{ expression: 'datediff(day, created_date, close_date)' }],
                        },
                    },
                },
            } as Partial<SchemaField>);
            expect(isCalculatedDimension(field)).toBe(true);
        });

        it('returns false when expression is missing or blank', () => {
            expect(isCalculatedDimension(buildField())).toBe(false);
            const blank = buildField({
                schemaFieldEntity: {
                    semanticFieldAnnotation: {
                        expression: { dialects: [{ expression: '  ' }] },
                    },
                },
            } as Partial<SchemaField>);
            expect(isCalculatedDimension(blank)).toBe(false);
        });
    });

    describe('getDimensionPillKind', () => {
        it('returns time for time dimensions', () => {
            const field = buildField({
                schemaFieldEntity: {
                    semanticFieldAnnotation: {
                        dimension: { isTime: true },
                    },
                },
            } as Partial<SchemaField>);
            expect(getDimensionPillKind(field)).toBe('time');
        });

        it('returns calculated before treating as plain', () => {
            const field = buildField({
                fieldPath: 'region_upper',
                schemaFieldEntity: {
                    semanticFieldAnnotation: {
                        expression: { dialects: [{ expression: 'upper(region)' }] },
                    },
                },
            } as Partial<SchemaField>);
            expect(getDimensionPillKind(field)).toBe('calculated');
        });

        it('returns plain for default dimensions', () => {
            expect(getDimensionPillKind(buildField())).toBe('plain');
        });
    });

    describe('getDimensionGroups', () => {
        it('keeps only datasets that have dimension fields', () => {
            const withDims = {
                urn: 'urn:li:dataset:1',
                type: EntityType.Dataset,
                name: 'opportunities',
                schema: {
                    fields: [
                        buildField({
                            schemaFieldEntity: {
                                semanticFieldAnnotation: { type: SemanticFieldType.Dimension },
                            },
                        } as Partial<SchemaField>),
                    ],
                },
            } as Dataset;
            const withoutDims = {
                urn: 'urn:li:dataset:2',
                type: EntityType.Dataset,
                name: 'targets',
                schema: {
                    fields: [
                        buildField({
                            fieldPath: 'amount',
                            schemaFieldEntity: {
                                semanticFieldAnnotation: { type: SemanticFieldType.Measure },
                            },
                        } as Partial<SchemaField>),
                    ],
                },
            } as Dataset;

            const groups = getDimensionGroups([withDims, withoutDims]);
            expect(groups).toHaveLength(1);
            expect(groups[0].dataset.urn).toBe('urn:li:dataset:1');
            expect(groups[0].fields).toHaveLength(1);
        });
    });
});
