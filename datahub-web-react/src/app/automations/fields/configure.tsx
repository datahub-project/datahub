import React, { createElement, useState, useEffect, useRef, useMemo } from 'react';
import _ from 'lodash';

import { getFields, flattenObject } from '@app/automations/utils';

import { useAutomationContext } from '../Automations/AutomationProvider';
import { updateRecipe } from '../Automations/utils/updateRecipe';
import { updateFormData } from '../Automations/utils/updateFormData';

import { Step, StepHeader, StepField } from './components';

interface Props {
    automation: any;
    isEdit?: boolean;
}

export const Configure = ({ automation, isEdit = false }: Props) => {
    const { formData, recipe, setRecipe, definition } = useAutomationContext();

    // Memoize steps to prevent unnecessary recalculations
    const steps = useMemo(() => automation.fields || getFields(automation), [automation]);

    // Collects any state changes from the form fields
    const [data, setData] = useState<any>({});

    // Use ref to skip initial effect execution
    const isInitialMount = useRef(true);

    // Handle updating the memorized state when a field component state changes
    const handleComponentStateChange = (componentName: string, newState: any) => {
        setData((prevData) => {
            const updatedData = { ...prevData, [componentName]: newState };
            return !_.isEqual(updatedData, prevData) ? updatedData : prevData;
        });
    };

    // Format the form data for the state
    const formatDataForState = (d: any) => {
        const dataObject: Record<string, any> = {};
        steps.forEach((step) => {
            step.fields.forEach((field) => {
                const componentName = field.component.name;
                const initialState = field.state || {};
                dataObject[componentName] = { ...initialState };
                Object.keys(initialState).forEach((key) => {
                    if (key in d) {
                        dataObject[componentName][key] = d[key];
                    }
                });
            });
        });
        return dataObject;
    };

    // Use effect that only runs once to update the form data in the context
    useEffect(() => {
        const initialFormattedData = flattenObject({ ...data, ...formData });
        const initialUpdatedFormData = updateFormData(definition, initialFormattedData);
        setData(formatDataForState(initialUpdatedFormData));
        // Mark the initial mount as done
        isInitialMount.current = false;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []); // Empty dependency array to ensure it only runs once

    // Send the form data back to the parent component
    useEffect(() => {
        if (isInitialMount.current) return; // Skip this effect on the initial mount

        const formattedFieldState = flattenObject(data);
        const updatedRecipe = updateRecipe(recipe, formattedFieldState);

        if (!_.isEqual(formData, formattedFieldState)) {
            setRecipe?.(updatedRecipe);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]); // Only run when `data` changes

    return (
        <div>
            {steps.map((step: any) => (
                <Step key={step.title}>
                    <StepHeader>
                        <h2>{step.title}</h2>
                        <p>{step.description}</p>
                    </StepHeader>
                    {step.fields.map((field: any) => {
                        const componentState = data[field.component.name] || {};
                        return (
                            <StepField key={field.component.name}>
                                {createElement(field.component, {
                                    state: componentState,
                                    props: { ...field.props, isEdit },
                                    passStateToParent: (newState) =>
                                        handleComponentStateChange(field.component.name, newState),
                                })}
                            </StepField>
                        );
                    })}
                </Step>
            ))}
        </div>
    );
};
