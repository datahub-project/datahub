import { shouldDisplayField } from '@app/workflows/utils/fieldConditions';

import {
    ActionWorkflowField,
    ActionWorkflowFieldConditionType,
    ActionWorkflowFieldValueType,
    FilterOperator,
    PropertyCardinality,
} from '@types';

describe('fieldConditions', () => {
    describe('shouldDisplayField', () => {
        it('should always display field when no condition is present', () => {
            const field: ActionWorkflowField = {
                id: 'test-field',
                name: 'Test Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: true,
                // No condition
            };

            const formValues = {};

            expect(shouldDisplayField(field, formValues)).toBe(true);
        });

        it('should display field when condition is met (EQUAL)', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'approval-type',
                        values: ['ADVANCED'],
                        condition: FilterOperator.Equal,
                        negated: false,
                    },
                },
            };

            const formValues = {
                'approval-type': ['ADVANCED'],
            };

            expect(shouldDisplayField(field, formValues)).toBe(true);
        });

        it('should hide field when condition is not met (EQUAL)', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'approval-type',
                        values: ['ADVANCED'],
                        condition: FilterOperator.Equal,
                        negated: false,
                    },
                },
            };

            const formValues = {
                'approval-type': ['BASIC'],
            };

            expect(shouldDisplayField(field, formValues)).toBe(false);
        });

        it('should handle negated conditions', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'approval-type',
                        values: ['BASIC'],
                        condition: FilterOperator.Equal,
                        negated: true, // Show when NOT equal to BASIC
                    },
                },
            };

            const formValues = {
                'approval-type': ['ADVANCED'],
            };

            expect(shouldDisplayField(field, formValues)).toBe(true);
        });

        it('should handle CONTAIN operator', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'description',
                        values: ['urgent'],
                        condition: FilterOperator.Contain,
                        negated: false,
                    },
                },
            };

            const formValues = {
                description: ['This is an urgent request'],
            };

            expect(shouldDisplayField(field, formValues)).toBe(true);
        });

        it('should handle multiple condition values', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'approval-type',
                        values: ['ADVANCED', 'CRITICAL'],
                        condition: FilterOperator.Equal,
                        negated: false,
                    },
                },
            };

            const formValues = {
                'approval-type': ['CRITICAL'],
            };

            expect(shouldDisplayField(field, formValues)).toBe(true);
        });

        it('should handle empty form values', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'approval-type',
                        values: ['ADVANCED'],
                        condition: FilterOperator.Equal,
                        negated: false,
                    },
                },
            };

            const formValues = {
                'approval-type': [],
            };

            expect(shouldDisplayField(field, formValues)).toBe(false);
        });

        it('should handle missing referenced field', () => {
            const field: ActionWorkflowField = {
                id: 'conditional-field',
                name: 'Conditional Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: false,
                condition: {
                    type: ActionWorkflowFieldConditionType.SingleFieldValue,
                    singleFieldValueCondition: {
                        field: 'missing-field',
                        values: ['ADVANCED'],
                        condition: FilterOperator.Equal,
                        negated: false,
                    },
                },
            };

            const formValues = {};

            expect(shouldDisplayField(field, formValues)).toBe(false);
        });
    });
});
