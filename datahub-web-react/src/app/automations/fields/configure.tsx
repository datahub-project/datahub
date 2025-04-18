/* eslint-disable react-hooks/exhaustive-deps, no-param-reassign */
import React, { createElement, useMemo } from 'react';
import { isEqual, camelCase, cloneDeep } from 'lodash';
import { useAutomationContext } from '../Automations/AutomationProvider';
import { Step, StepHeader, StepField } from './components';

interface Props {
    isEdit?: boolean;
}

export const Configure = ({ isEdit = false }: Props) => {
    const { formState, typeTemplate, setFormState } = useAutomationContext();

    // Compute the fields that should be displayed in the form, based on the recipe type template.
    const allFields = useMemo(() => {
        if (typeTemplate) {
            // The default recipe fields come from the type template, which defined the fields to be collected.
            const recipeFields = cloneDeep(typeTemplate.fields);

            // Add unique keys to each step & field component
            (recipeFields || []).forEach((step, i) => {
                step.key = `${camelCase(step.title)}_${i}`;
                (step.fields || []).forEach((field) => {
                    // Create a unique id for the field component being rendered, mainly to support non-conflict with multiple fields
                    // of the same component type.
                    // Currently, you cannot have 2 nonConditional fields of the same name. TODO: Think about how to make these globally unique.
                    field.key = `${camelCase(field.component.name)}-${camelCase(
                        step.conditionalKey || 'nonConditional',
                    )}`;
                });
            });

            return recipeFields;
        }
        return [];
    }, [typeTemplate]);

    // Conditional steps (steps that have the key `conditionalKey`)
    const conditionalFields = allFields.filter((step) => step.conditionalKey);

    // Memoized field steps, including conditionally rendered steps based on `fieldState`
    // Initializing the "local" component state for each field that is currently visible.
    const visibleFields = useMemo(() => {
        const newVisibleFields: any[] = [];

        // Determine if some of the fields are now visible, due to the updated state change!
        allFields.forEach((field) => {
            if (!field.conditionalKey) {
                newVisibleFields.push(field); // Add the current step if it's not a conditional step
            } else {
                // If this step has a `controlKey`, check for conditional steps
                const { controlKey } = field;
                if (controlKey) {
                    const conditionalFieldsForThisControl = conditionalFields.filter(
                        (conditionalStep) => conditionalStep.controlKey === controlKey,
                    );
                    conditionalFieldsForThisControl.forEach((conditionalStep) => {
                        const canConditionallyRender = Object.values(formState).includes(
                            conditionalStep.conditionalKey,
                        );

                        if (
                            canConditionallyRender &&
                            !newVisibleFields.some((visibleStep) => visibleStep.title === conditionalStep.title)
                        ) {
                            newVisibleFields.push(conditionalStep);
                        }
                    });
                }
            }
        });

        return newVisibleFields;
    }, [formState, allFields]); // Only recalculate `visibleFields` when `formState` changes

    // Handle updating the global form state when a field makes a change.
    const handleSingleFieldStateChange = (newPartialState: any) => {
        const newFormState = {
            ...formState,
            ...newPartialState,
        };

        // Only set form state if this version is different from the context
        if (!isEqual(formState, newFormState)) {
            setFormState?.(newFormState);
        }
    };

    return (
        <div>
            {visibleFields.map((step: any) => {
                return (
                    <Step key={step.key}>
                        {!step.isHidden && (
                            <StepHeader>
                                <h2>{step.title}</h2>
                                <p>{step.description}</p>
                            </StepHeader>
                        )}
                        {step.fields.map((field: any) => {
                            const componentKey = field.key;
                            const props = { ...field.props, isEdit };

                            return (
                                <StepField key={componentKey}>
                                    {createElement(field.component, {
                                        state: formState,
                                        props,
                                        passStateToParent: (newState) => handleSingleFieldStateChange(newState),
                                    })}
                                </StepField>
                            );
                        })}
                    </Step>
                );
            })}
        </div>
    );
};
