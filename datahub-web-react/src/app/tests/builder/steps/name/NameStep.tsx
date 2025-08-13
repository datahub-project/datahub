import { Form, Input, Typography, message } from 'antd';
import React, { useState, useMemo } from 'react';
import styled from 'styled-components';

import { CategorySelect } from '@app/tests/builder/steps/name/CategorySelect';
import { StepProps } from '@app/tests/builder/types';
import { DEFAULT_TEST_CATEGORY, TestCategory } from '@app/tests/constants';
import { isCustomCategory, isSupportedCategory } from '@app/tests/utils';
import { Button } from '@src/alchemy-components';
import { ValidationWarning } from '@app/tests/builder/validation/ValidationWarning';
import { getValidationWarnings, getPropertiesFromPredicate } from '@app/tests/builder/validation/utils';
import { deserializeTestDefinition } from '@app/tests/builder/steps/definition/utils';
import { convertTestPredicateToLogicalPredicate } from '@app/tests/builder/steps/definition/builder/utils';
import { graphNamesToEntityTypes } from '@app/tests/builder/steps/select/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

const StyledForm = styled(Form)`
    max-width: 400px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const SaveButton = styled(Button)`
    margin-right: 15px;
`;

export const NameStep = ({ state, updateState, prev, submit }: StepProps) => {
    const entityRegistry = useEntityRegistry();
    const [showCustomCategory, setShowCustomCategory] = useState(isCustomCategory(state?.category));
    
    // Get comprehensive validation for the entire test definition
    const testDefinition = useMemo(() => deserializeTestDefinition(state?.definition?.json || '{}'), [state]);
    const selectedEntityTypes = graphNamesToEntityTypes(testDefinition.on?.types || [], entityRegistry);
    
    // Get validation warnings (memoized for proper re-evaluation)
    const validationWarnings = useMemo(() => {
        // Validate selection filters
        const selectionPredicate = convertTestPredicateToLogicalPredicate(testDefinition.on?.conditions || []) as LogicalPredicate;
        const selectionProperties = getPropertiesFromPredicate(selectionPredicate);
        
        // Validate rules
        const rulesPredicate = convertTestPredicateToLogicalPredicate(testDefinition.rules) as LogicalPredicate;
        const rulesProperties = getPropertiesFromPredicate(rulesPredicate);
        
        // Validate actions
        const allActions = [
            ...(testDefinition.actions?.passing || []),
            ...(testDefinition.actions?.failing || []),
        ];
        
        // Get all validation warnings
        const allProperties = [...selectionProperties, ...rulesProperties];
        return getValidationWarnings(selectedEntityTypes, allProperties, allActions);
    }, [
        selectedEntityTypes.join(','), // Convert array to string for stable comparison
        JSON.stringify(testDefinition.on?.conditions || []), // Stringify for stable comparison
        JSON.stringify(testDefinition.rules), // Stringify for stable comparison
        JSON.stringify(testDefinition.actions), // Stringify for stable comparison
        JSON.stringify(testDefinition.on?.types || []), // Include types for entity changes
    ]);

    const setName = (name: string) => {
        updateState({
            ...state,
            name,
        });
    };

    const setCategory = (category: string) => {
        if (category === TestCategory.CUSTOM || !isSupportedCategory(category)) {
            setShowCustomCategory(true);
        } else {
            setShowCustomCategory(false);
        }
        updateState({
            ...state,
            category,
        });
    };

    const setDescription = (description: string) => {
        updateState({
            ...state,
            description,
        });
    };

    const onClickCreate = () => {
        if (state.category?.length && state.name?.length) {
            // Check for validation warnings before saving
            if (validationWarnings.length > 0) {
                message.error(
                    'Cannot save test with invalid configuration. Please fix the validation warnings before saving.',
                    5
                );
                return;
            }
            submit();
        }
    };

    return (
        <>
            {/* Show validation warnings if any */}
            {validationWarnings.length > 0 && (
                <ValidationWarning
                    key={`validation-final-${selectedEntityTypes.join('-')}-${validationWarnings.length}`}
                    warnings={validationWarnings}
                    showResetFilters={false}
                    showResetActions={false}
                />
            )}
            
            <StyledForm layout="vertical">
                <Form.Item required label={<Typography.Text strong>Name</Typography.Text>}>
                    <Input
                        placeholder="A name for your test"
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item required label={<Typography.Text strong>Category</Typography.Text>}>
                    <CategorySelect
                        categoryName={state.category || DEFAULT_TEST_CATEGORY}
                        onSelect={(newValue) => setCategory(newValue)}
                    />
                </Form.Item>
                {showCustomCategory && (
                    <Form.Item required label={<Typography.Text strong>Custom Category</Typography.Text>}>
                        <Input
                            placeholder="The category of your test"
                            value={state.category}
                            onChange={(event) => setCategory(event.target.value)}
                        />
                    </Form.Item>
                )}
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Input.TextArea
                        placeholder="The description for your test"
                        value={state.description || undefined}
                        onChange={(event) => setDescription(event.target.value)}
                    />
                </Form.Item>
            </StyledForm>
            <ControlsContainer>
                <Button variant="outline" color="gray" onClick={prev}>
                    Back
                </Button>
                <SaveButton
                    disabled={!(state.name !== undefined && state.name.length > 0)}
                    onClick={() => onClickCreate()}
                >
                    Save
                </SaveButton>
            </ControlsContainer>
        </>
    );
};
