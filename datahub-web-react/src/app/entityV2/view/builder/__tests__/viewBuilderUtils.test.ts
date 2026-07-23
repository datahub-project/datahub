import { BUILD_FILTERS_TAB_KEY, SELECT_ASSETS_TAB_KEY, URN_FILTER_NAME } from '@app/entityV2/view/builder/constants';
import {
    buildEntityMap,
    buildViewDefinition,
    filtersToLogicalPredicate,
    filtersToSelectedUrns,
    getInitialTabKey,
    logicalPredicateToFilters,
    mapConditionToUiOperator,
    mapUiOperatorToCondition,
    selectedUrnsToFilters,
} from '@app/entityV2/view/builder/utils';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { EntityType, FilterOperator, LogicalOperator } from '@types';

describe('View builder conversion utils', () => {
    describe('selectedUrnsToFilters', () => {
        it('should store all URNs under the urn filter field', () => {
            const selectedUrns = ['urn:li:domain:marketing', 'urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'];

            const result = selectedUrnsToFilters(selectedUrns);

            expect(result).toEqual([
                {
                    field: URN_FILTER_NAME,
                    values: ['urn:li:domain:marketing', 'urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'],
                },
            ]);
        });

        it('should return empty array when no URNs are provided', () => {
            expect(selectedUrnsToFilters([])).toEqual([]);
        });

        it('should produce a single urn filter regardless of entity types', () => {
            const selectedUrns = [
                'urn:li:domain:marketing',
                'urn:li:tag:pii',
                'urn:li:corpuser:john',
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.table,PROD)',
            ];

            const result = selectedUrnsToFilters(selectedUrns);

            expect(result).toHaveLength(1);
            expect(result[0].field).toBe(URN_FILTER_NAME);
            expect(result[0].values).toHaveLength(4);
        });
    });

    describe('logicalPredicateToFilters', () => {
        it('should return empty filters for null predicate', () => {
            const result = logicalPredicateToFilters(null);
            expect(result).toEqual({ operator: LogicalOperator.And, filters: [] });
        });

        it('should return empty filters for undefined predicate', () => {
            const result = logicalPredicateToFilters(undefined);
            expect(result).toEqual({ operator: LogicalOperator.And, filters: [] });
        });

        it('should convert AND predicate with property operands and preserve operators', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    { type: 'property', property: 'domains', operator: 'equals', values: ['urn:li:domain:marketing'] },
                    { type: 'property', property: 'tags', operator: 'exists', values: [] },
                ],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.operator).toBe(LogicalOperator.And);
            expect(result.filters).toHaveLength(2);
            expect(result.filters[0].field).toBe('domains');
            expect(result.filters[0].condition).toBe(FilterOperator.Equal);
            expect(result.filters[1].field).toBe('tags');
            expect(result.filters[1].condition).toBe(FilterOperator.Exists);
        });

        it('should convert OR predicate', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [
                    { type: 'property', property: 'domains', operator: 'equals', values: ['urn:li:domain:marketing'] },
                ],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.operator).toBe(LogicalOperator.Or);
        });

        it('should flatten nested groups into a flat filter list', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property',
                                property: 'domains',
                                operator: 'equals',
                                values: ['urn:li:domain:marketing'],
                            },
                        ],
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.AND,
                        operands: [
                            {
                                type: 'property',
                                property: 'tags',
                                operator: 'equals',
                                values: ['urn:li:tag:pii'],
                            },
                        ],
                    },
                ],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.filters).toHaveLength(2);
            expect(result.filters[0].field).toBe('domains');
            expect(result.filters[1].field).toBe('tags');
        });

        it('should inject boolean values for is_true/is_false operators', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    { type: 'property', property: 'hasDescription', operator: 'is_true', values: [] },
                    { type: 'property', property: 'removed', operator: 'is_false', values: [] },
                ],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.filters).toHaveLength(2);
            expect(result.filters[0].field).toBe('hasDescription');
            expect(result.filters[0].values).toEqual(['true']);
            expect(result.filters[0].condition).toBe(FilterOperator.Equal);
            expect(result.filters[1].field).toBe('removed');
            expect(result.filters[1].values).toEqual(['false']);
            expect(result.filters[1].condition).toBe(FilterOperator.Equal);
        });

        it('should set negated on filters inside a NOT predicate', () => {
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [
                    { type: 'property', property: 'domains', operator: 'equals', values: ['urn:li:domain:marketing'] },
                ],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.filters).toHaveLength(1);
            expect(result.filters[0].negated).toBe(true);
        });

        it('should cancel negation when a NOT group wraps a not_equals row (double negation)', () => {
            // A "None" group over a "does not equal" row is NOT(NOT x), which must
            // collapse back to a plain equals — the group NOT and the per-row
            // not_equals cancel via XOR rather than stacking.
            const predicate: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.NOT,
                operands: [{ type: 'property', property: 'tags', operator: 'not_equals', values: ['urn:li:tag:pii'] }],
            };

            const result = logicalPredicateToFilters(predicate);

            expect(result.filters).toHaveLength(1);
            expect(result.filters[0].field).toBe('tags');
            expect(result.filters[0].condition).toBe(FilterOperator.Equal);
            expect(result.filters[0].negated).toBeUndefined();
        });
    });

    describe('filtersToSelectedUrns', () => {
        it('should extract URNs from the urn filter field', () => {
            const filters = [
                {
                    field: URN_FILTER_NAME,
                    values: ['urn:li:domain:marketing', 'urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'],
                },
            ];

            const result = filtersToSelectedUrns(filters);

            expect(result).toEqual(['urn:li:domain:marketing', 'urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)']);
        });

        it('should return empty array when no urn filter exists', () => {
            const filters = [
                { field: '_entityType', values: ['DATASETS'] },
                { field: 'domains', values: ['urn:li:domain:marketing'] },
            ];

            expect(filtersToSelectedUrns(filters)).toEqual([]);
        });

        it('should return empty array for empty filters', () => {
            expect(filtersToSelectedUrns([])).toEqual([]);
        });

        it('should ignore non-urn filter fields', () => {
            const filters = [
                { field: 'domains', values: ['urn:li:domain:marketing'] },
                { field: 'tags', values: ['urn:li:tag:pii'] },
                { field: URN_FILTER_NAME, values: ['urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'] },
            ];

            const result = filtersToSelectedUrns(filters);

            expect(result).toEqual(['urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)']);
        });
    });

    describe('filtersToLogicalPredicate', () => {
        it('should convert AND filters to a LogicalPredicate', () => {
            const filters = [
                { field: 'domains', values: ['urn:li:domain:marketing'] },
                { field: 'tags', values: ['urn:li:tag:pii'] },
            ];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(2);
        });

        it('should convert OR operator', () => {
            const filters = [{ field: 'domains', values: ['urn:li:domain:marketing'] }];
            const result = filtersToLogicalPredicate(LogicalOperator.Or, filters);
            expect(result.operator).toBe(LogicalOperatorType.OR);
        });

        it('should default to AND when operator is undefined', () => {
            const result = filtersToLogicalPredicate(undefined, []);
            expect(result.operator).toBe(LogicalOperatorType.AND);
            expect(result.operands).toHaveLength(0);
        });

        it('should restore UI operator from filter condition', () => {
            const filters = [
                { field: 'domains', values: ['urn:li:domain:marketing'], condition: FilterOperator.Equal },
                { field: 'tags', values: [], condition: FilterOperator.Exists },
            ];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            expect(result.operands).toHaveLength(2);
            const op0 = result.operands[0] as { operator?: string };
            const op1 = result.operands[1] as { operator?: string };
            expect(op0.operator).toBe('equals');
            expect(op1.operator).toBe('exists');
        });

        it('should represent negated filters with the per-row not_equals operator', () => {
            const filters = [
                {
                    field: 'domains',
                    values: ['urn:li:domain:marketing'],
                    condition: FilterOperator.Equal,
                    negated: true,
                },
                { field: 'tags', values: ['urn:li:tag:pii'], condition: FilterOperator.Equal, negated: false },
            ];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            // Group operator stays AND; negation is carried per-row, not as a NOT group.
            expect(result.operator).toBe(LogicalOperatorType.AND);
            const op0 = result.operands[0] as { operator?: string };
            const op1 = result.operands[1] as { operator?: string };
            expect(op0.operator).toBe('not_equals');
            expect(op1.operator).toBe('equals');
        });

        it('should seed a leading _entityType row from the top-level entityTypes', () => {
            const filters = [{ field: 'tags', values: ['urn:li:tag:pii'], condition: FilterOperator.Equal }];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters, [
                EntityType.Dataset,
                EntityType.Dashboard,
            ]);

            expect(result.operands).toHaveLength(2);
            const entityTypeRow = result.operands[0] as { property?: string; operator?: string; values?: string[] };
            expect(entityTypeRow.property).toBe('_entityType');
            expect(entityTypeRow.operator).toBe('equals');
            expect(entityTypeRow.values).toEqual([EntityType.Dataset, EntityType.Dashboard]);
        });

        it('should default to equals when condition is undefined', () => {
            const filters = [{ field: 'domains', values: ['urn:li:domain:marketing'] }];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            const op0 = result.operands[0] as { operator?: string };
            expect(op0.operator).toBe('equals');
        });

        it('should restore is_true/is_false and clear boolean values', () => {
            const filters = [
                { field: 'hasDescription', values: ['true'], condition: FilterOperator.Equal },
                { field: 'removed', values: ['false'], condition: FilterOperator.Equal },
            ];

            const result = filtersToLogicalPredicate(LogicalOperator.And, filters);

            const op0 = result.operands[0] as { operator?: string; values?: string[] };
            const op1 = result.operands[1] as { operator?: string; values?: string[] };
            expect(op0.operator).toBe('is_true');
            expect(op0.values).toEqual([]);
            expect(op1.operator).toBe('is_false');
            expect(op1.values).toEqual([]);
        });
    });

    describe('getInitialTabKey', () => {
        it('should return Build Filters tab when filters are empty', () => {
            expect(getInitialTabKey([])).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should return Select Assets tab when filters contain the "urn" field', () => {
            const filters = [
                { field: URN_FILTER_NAME, values: ['urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'] },
            ];
            expect(getInitialTabKey(filters)).toBe(SELECT_ASSETS_TAB_KEY);
        });

        it('should return Build Filters tab for domain filters', () => {
            const filters = [{ field: 'domains', values: ['urn:li:domain:marketing'] }];
            expect(getInitialTabKey(filters)).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should return Build Filters tab for dynamic-only fields', () => {
            const filters = [{ field: '_entityType', values: ['DATASETS'] }];
            expect(getInitialTabKey(filters)).toBe(BUILD_FILTERS_TAB_KEY);
        });

        it('should return Select Assets tab when urn field is mixed with other fields', () => {
            const filters = [
                { field: URN_FILTER_NAME, values: ['urn:li:dataset:(urn:li:dataPlatform:bigquery,t,PROD)'] },
                { field: 'domains', values: ['urn:li:domain:marketing'] },
            ];
            expect(getInitialTabKey(filters)).toBe(SELECT_ASSETS_TAB_KEY);
        });
    });

    describe('buildViewDefinition', () => {
        it('should build a valid view definition with AND operator', () => {
            const filters = [{ field: URN_FILTER_NAME, values: ['urn:li:domain:marketing'] }];

            const result = buildViewDefinition(LogicalOperator.And, filters);

            expect(result.entityTypes).toEqual([]);
            expect(result.filter?.operator).toBe(LogicalOperator.And);
            expect(result.filter?.filters).toHaveLength(1);
        });

        it('should lift _entityType rows into the top-level entityTypes and drop them from filters', () => {
            const filters = [
                { field: '_entityType', values: [EntityType.Dataset, EntityType.Container] },
                { field: 'tags', values: ['urn:li:tag:pii'], condition: FilterOperator.Equal },
            ];

            const result = buildViewDefinition(LogicalOperator.And, filters);

            expect(result.entityTypes).toEqual([EntityType.Dataset, EntityType.Container]);
            expect(result.filter?.filters).toHaveLength(1);
            expect(result.filter?.filters?.[0]?.field).toBe('tags');
        });
    });

    describe('mapUiOperatorToCondition', () => {
        it('should map known UI operators to backend FilterOperator', () => {
            expect(mapUiOperatorToCondition('equals')).toBe(FilterOperator.Equal);
            expect(mapUiOperatorToCondition('exists')).toBe(FilterOperator.Exists);
            expect(mapUiOperatorToCondition('contains_str')).toBe(FilterOperator.Contain);
            expect(mapUiOperatorToCondition('greater_than')).toBe(FilterOperator.GreaterThan);
            expect(mapUiOperatorToCondition('less_than')).toBe(FilterOperator.LessThan);
        });

        it('should map boolean operators to Equal', () => {
            expect(mapUiOperatorToCondition('is_true')).toBe(FilterOperator.Equal);
            expect(mapUiOperatorToCondition('is_false')).toBe(FilterOperator.Equal);
        });

        it('should map not_equals to Equal (negation carried by the negated flag)', () => {
            expect(mapUiOperatorToCondition('not_equals')).toBe(FilterOperator.Equal);
        });

        it('should return undefined for undefined input', () => {
            expect(mapUiOperatorToCondition(undefined)).toBeUndefined();
        });

        it('should fall back to Equal for unknown operators', () => {
            expect(mapUiOperatorToCondition('some_unknown_op')).toBe(FilterOperator.Equal);
        });
    });

    describe('mapConditionToUiOperator', () => {
        it('should map known backend conditions to UI operators', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal)).toBe('equals');
            expect(mapConditionToUiOperator(FilterOperator.Exists)).toBe('exists');
            expect(mapConditionToUiOperator(FilterOperator.Contain)).toBe('contains_str');
            expect(mapConditionToUiOperator(FilterOperator.GreaterThan)).toBe('greater_than');
        });

        it('should detect is_true from Equal condition with value ["true"]', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['true'])).toBe('is_true');
        });

        it('should detect is_false from Equal condition with value ["false"]', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['false'])).toBe('is_false');
        });

        it('should return equals for Equal condition with non-boolean values', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['urn:li:domain:marketing'])).toBe('equals');
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['true', 'false'])).toBe('equals');
        });

        it('should default to equals for null/undefined', () => {
            expect(mapConditionToUiOperator(null)).toBe('equals');
            expect(mapConditionToUiOperator(undefined)).toBe('equals');
        });

        it('should map a negated Equal condition to not_equals', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['urn:li:tag:pii'], true)).toBe('not_equals');
        });

        it('should not treat a non-negated Equal as not_equals', () => {
            expect(mapConditionToUiOperator(FilterOperator.Equal, ['urn:li:tag:pii'], false)).toBe('equals');
        });
    });

    describe('round-trip: logicalPredicateToFilters → filtersToLogicalPredicate', () => {
        it('should preserve operators through a full save/load cycle', () => {
            const original: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    { type: 'property', property: 'domains', operator: 'equals', values: ['urn:li:domain:finance'] },
                    { type: 'property', property: 'platform', operator: 'exists', values: [] },
                ],
            };

            const { operator, filters } = logicalPredicateToFilters(original);
            const restored = filtersToLogicalPredicate(operator, filters);

            expect(restored.operator).toBe(LogicalOperatorType.AND);
            expect(restored.operands).toHaveLength(2);
            const op0 = restored.operands[0] as { property?: string; operator?: string; values?: string[] };
            const op1 = restored.operands[1] as { property?: string; operator?: string; values?: string[] };
            expect(op0.property).toBe('domains');
            expect(op0.operator).toBe('equals');
            expect(op0.values).toEqual(['urn:li:domain:finance']);
            expect(op1.property).toBe('platform');
            expect(op1.operator).toBe('exists');
        });

        it('should preserve entityTypes and per-row negation across a full load → save cycle', () => {
            // Mirrors the reported regression: a view scoped to several entity
            // types with a single "tag does not equal" filter.
            const savedEntityTypes = [
                EntityType.Dataset,
                EntityType.Dashboard,
                EntityType.Chart,
                EntityType.Container,
                EntityType.DataProduct,
            ];
            const savedFilters = [
                {
                    field: 'tags',
                    values: ['urn:li:tag:private'],
                    condition: FilterOperator.Equal,
                    negated: true,
                },
            ];

            // Load into the builder.
            const predicate = filtersToLogicalPredicate(LogicalOperator.Or, savedFilters, savedEntityTypes);
            const entityTypeRow = predicate.operands[0] as { property?: string; operator?: string; values?: string[] };
            const tagRow = predicate.operands[1] as { property?: string; operator?: string };
            expect(entityTypeRow.property).toBe('_entityType');
            expect(entityTypeRow.values).toEqual(savedEntityTypes);
            expect(tagRow.property).toBe('tags');
            expect(tagRow.operator).toBe('not_equals');

            // Save back out.
            const { operator, filters } = logicalPredicateToFilters(predicate);
            const definition = buildViewDefinition(operator, filters);

            expect(definition.entityTypes).toEqual(savedEntityTypes);
            expect(definition.filter?.operator).toBe(LogicalOperator.Or);
            expect(definition.filter?.filters).toHaveLength(1);
            const savedTag = definition.filter?.filters?.[0];
            expect(savedTag?.field).toBe('tags');
            expect(savedTag?.condition).toBe(FilterOperator.Equal);
            expect(savedTag?.negated).toBe(true);
            expect(savedTag?.values).toEqual(['urn:li:tag:private']);
        });

        it('should preserve a mix of negated and non-negated filters across a load → save cycle', () => {
            // The exact master regression: the old "wrap in NOT only if every
            // filter is negated" heuristic dropped negation on load whenever the
            // set was mixed, so re-saving lost the per-row negated flags.
            const savedFilters = [
                { field: 'domains', values: ['urn:li:domain:marketing'], condition: FilterOperator.Equal, negated: true },
                { field: 'tags', values: ['urn:li:tag:pii'], condition: FilterOperator.Equal, negated: false },
            ];

            // Load: negation is carried per-row via not_equals, group stays AND.
            const predicate = filtersToLogicalPredicate(LogicalOperator.And, savedFilters);
            expect(predicate.operator).toBe(LogicalOperatorType.AND);
            const domainRow = predicate.operands[0] as { property?: string; operator?: string };
            const tagRow = predicate.operands[1] as { property?: string; operator?: string };
            expect(domainRow.operator).toBe('not_equals');
            expect(tagRow.operator).toBe('equals');

            // Save: each row keeps its own negation independently.
            const { operator, filters } = logicalPredicateToFilters(predicate);
            const definition = buildViewDefinition(operator, filters);

            expect(definition.filter?.filters).toHaveLength(2);
            const domain = definition.filter?.filters?.find((f) => f.field === 'domains');
            const tag = definition.filter?.filters?.find((f) => f.field === 'tags');
            expect(domain?.condition).toBe(FilterOperator.Equal);
            expect(domain?.negated).toBe(true);
            expect(tag?.condition).toBe(FilterOperator.Equal);
            expect(tag?.negated).toBeFalsy();
        });

        it('should round-trip boolean is_true/is_false operators correctly', () => {
            const original: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    { type: 'property', property: 'hasDescription', operator: 'is_true', values: [] },
                    { type: 'property', property: 'removed', operator: 'is_false', values: [] },
                ],
            };

            const { operator, filters } = logicalPredicateToFilters(original);

            expect(filters[0].values).toEqual(['true']);
            expect(filters[0].condition).toBe(FilterOperator.Equal);
            expect(filters[1].values).toEqual(['false']);
            expect(filters[1].condition).toBe(FilterOperator.Equal);

            const restored = filtersToLogicalPredicate(operator, filters);
            const op0 = restored.operands[0] as { property?: string; operator?: string; values?: string[] };
            const op1 = restored.operands[1] as { property?: string; operator?: string; values?: string[] };

            expect(op0.operator).toBe('is_true');
            expect(op0.values).toEqual([]);
            expect(op1.operator).toBe('is_false');
            expect(op1.values).toEqual([]);
        });
    });

    describe('buildEntityMap', () => {
        it('should build a map keyed by URN', () => {
            const entities = [
                { urn: 'urn:li:domain:marketing', type: 'DOMAIN' },
                { urn: 'urn:li:tag:pii', type: 'TAG' },
            ];

            const result = buildEntityMap(entities);

            expect(result['urn:li:domain:marketing']).toEqual({ urn: 'urn:li:domain:marketing', type: 'DOMAIN' });
            expect(result['urn:li:tag:pii']).toEqual({ urn: 'urn:li:tag:pii', type: 'TAG' });
        });

        it('should return an empty map for an empty array', () => {
            expect(buildEntityMap([])).toEqual({});
        });

        it('should use the last entity when URNs are duplicated', () => {
            const entities = [
                { urn: 'urn:li:domain:a', name: 'first' },
                { urn: 'urn:li:domain:a', name: 'second' },
            ];

            const result = buildEntityMap(entities);

            expect(result['urn:li:domain:a'].name).toBe('second');
        });
    });
});
