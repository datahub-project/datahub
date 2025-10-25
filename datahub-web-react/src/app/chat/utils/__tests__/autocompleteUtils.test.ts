import { flattenAutocompleteSuggestions } from '@app/chat/utils/autocompleteUtils';

import { Entity, EntityType } from '@types';

describe('flattenAutocompleteSuggestions', () => {
    it('should return empty array for empty input', () => {
        expect(flattenAutocompleteSuggestions([])).toEqual([]);
    });

    it('should return empty array for null or undefined input', () => {
        expect(flattenAutocompleteSuggestions(null as any)).toEqual([]);
        expect(flattenAutocompleteSuggestions(undefined as any)).toEqual([]);
    });

    it('should flatten suggestions with single entity', () => {
        const mockEntity: Entity = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,test_table,PROD)',
            type: EntityType.Dataset,
        };

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['test_table'],
                entities: [mockEntity],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockEntity);
    });

    it('should flatten suggestions with multiple entities', () => {
        const mockEntities: Entity[] = [
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table1,PROD)',
                type: EntityType.Dataset,
            },
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table2,PROD)',
                type: EntityType.Dataset,
            },
        ];

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table1', 'table2'],
                entities: mockEntities,
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(2);
        expect(result).toEqual(mockEntities);
    });

    it('should flatten suggestions with multiple entity types', () => {
        const datasetEntity: Entity = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
            type: EntityType.Dataset,
        };

        const chartEntity: Entity = {
            urn: 'urn:li:chart:(urn:li:dataPlatform:looker,chart_id,PROD)',
            type: EntityType.Chart,
        };

        const dashboardEntity: Entity = {
            urn: 'urn:li:dashboard:(urn:li:dataPlatform:looker,dashboard_id,PROD)',
            type: EntityType.Dashboard,
        };

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: [datasetEntity],
            },
            {
                type: 'CHART',
                suggestions: ['chart_id'],
                entities: [chartEntity],
            },
            {
                type: 'DASHBOARD',
                suggestions: ['dashboard_id'],
                entities: [dashboardEntity],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(3);
        expect(result).toEqual([datasetEntity, chartEntity, dashboardEntity]);
    });

    it('should handle suggestions with empty entities array', () => {
        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: [],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toEqual([]);
    });

    it('should handle suggestions with null entities', () => {
        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: null,
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toEqual([]);
    });

    it('should handle suggestions with undefined entities', () => {
        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: undefined,
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toEqual([]);
    });

    it('should handle mixed valid and invalid suggestions', () => {
        const validEntity: Entity = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
            type: EntityType.Dataset,
        };

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: [validEntity],
            },
            {
                type: 'CHART',
                suggestions: ['chart'],
                entities: null,
            },
            {
                type: 'DASHBOARD',
                suggestions: ['dashboard'],
                entities: [],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(validEntity);
    });

    it('should handle complex real-world scenario', () => {
        const entities: Entity[] = [
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)',
                type: EntityType.Dataset,
            },
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)',
                type: EntityType.Dataset,
            },
            {
                urn: 'urn:li:chart:(urn:li:dataPlatform:looker,user_analytics,PROD)',
                type: EntityType.Chart,
            },
            {
                urn: 'urn:li:dashboard:(urn:li:dataPlatform:looker,executive_dashboard,PROD)',
                type: EntityType.Dashboard,
            },
            {
                urn: 'urn:li:dataJob:(urn:li:dataFlow:(airflow,user_processing,PROD),task_1)',
                type: EntityType.DataJob,
            },
        ];

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['users', 'orders'],
                entities: [entities[0], entities[1]],
            },
            {
                type: 'CHART',
                suggestions: ['user_analytics'],
                entities: [entities[2]],
            },
            {
                type: 'DASHBOARD',
                suggestions: ['executive_dashboard'],
                entities: [entities[3]],
            },
            {
                type: 'DATA_JOB',
                suggestions: ['user_processing'],
                entities: [entities[4]],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(5);
        expect(result).toEqual(entities);
    });

    it('should preserve entity order from suggestions', () => {
        const entities: Entity[] = [
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,first,PROD)',
                type: EntityType.Dataset,
            },
            {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,second,PROD)',
                type: EntityType.Dataset,
            },
            {
                urn: 'urn:li:chart:(urn:li:dataPlatform:looker,third,PROD)',
                type: EntityType.Chart,
            },
        ];

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['first', 'second'],
                entities: [entities[0], entities[1]],
            },
            {
                type: 'CHART',
                suggestions: ['third'],
                entities: [entities[2]],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toEqual(entities);
    });

    it('should handle entities with additional properties', () => {
        const entityWithProperties: Entity = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)',
            type: EntityType.Dataset,
            // Additional properties that might be present in real entities
            properties: {
                name: 'table',
                description: 'Test table',
            },
        } as any;

        const suggestions = [
            {
                type: 'DATASET',
                suggestions: ['table'],
                entities: [entityWithProperties],
            },
        ];

        const result = flattenAutocompleteSuggestions(suggestions);

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(entityWithProperties);
    });
});
