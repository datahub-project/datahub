/* eslint-disable */
// TODO: Cleanup linting errors

import React, { useState, useEffect, useRef } from 'react';

import { Input } from 'antd';

import { Step, StepHeader, StepField } from './components';

import { getSteps } from './utils';

import { TermSelector } from './fields/TermSelector';
import { ConnectionSelector } from './fields/ConnectionSelector';
import { CategorySelector } from './fields/CategorySelector';
import { TraversalSelector } from './fields/TraversalSelector';
import { CustomActionSelector } from './fields/CustomActionSelector';
import { DataAssetSelector } from './fields/DataAssetSelector';
import { ConditionSelector } from './fields/ConditionSelector';

export type FormDataType = {
    terms: string[];
    connection: string | undefined;
    conditions: string[];
    actions: string[];
    category: string | undefined;
    source: string[];
    name: string | undefined;
    description: string | undefined;
};

interface Props {
    automation: any;
    formData: FormDataType;
    setFormData: (data: FormDataType) => void;
}

export const Configure = ({ automation, formData, setFormData }: Props) => {
    const steps = automation.steps || getSteps(automation);
    const prevProps = useRef(formData);

    // Various field states
    const [actionSelection, setActionSelection] = useState<string[]>(formData.actions || []);
    const [conditionSelection, setConditionSelection] = useState<string[]>([]);
    const [initialConditions, setInitialConditions] = useState<string[]>(formData.conditions || []);
    const [assetTypesSelected, setAssetTypesSelected] = useState<string[]>(formData.source || []);
    const [termsSelected, setTermsSelected] = useState<string[]>(formData.terms || automation.terms || []);
    const [connectionSelected, setConnectionSelected] = useState<string | undefined>(formData.connection);
    const [categorySelected, setCategorySelected] = useState<string | undefined>(
        formData.category || automation.category,
    );
    const [details, setDetails] = useState<any>({
        name: formData.name || automation.name,
        description: formData.description || automation.description,
    });

    // Form Data to be submitted
    const data: FormDataType = {
        terms: termsSelected,
        connection: connectionSelected,
        conditions: conditionSelection,
        actions: actionSelection,
        category: categorySelected,
        source: assetTypesSelected,
        ...details,
    };

    // Send the form data back to the parent component
    // Only sends the data if the form data has changed
    useEffect(() => {
        const prevData = prevProps.current;
        const hasChanged = JSON.stringify(prevData) !== JSON.stringify(data);
        if (hasChanged) setFormData(data);
        prevProps.current = data;
    }, [data]);

    return (
        <div>
            {steps.map((step: any, index: number) => (
                <Step key={index}>
                    {/* Header */}
                    <StepHeader>
                        <h2>{step.title}</h2>
                        <p>{step.description}</p>
                    </StepHeader>

                    {/* Fields */}
                    {step.fields.map((field: any, index: number) => {
                        return (
                            <StepField key={index}>
                                {/* Field Label */}
                                {field.label && <label>{field.label}</label>}

                                {/* Term Selector */}
                                {field.type === 'termSelector' && (
                                    <TermSelector
                                        termsSelected={termsSelected}
                                        setTermsSelected={setTermsSelected}
                                        isRequired={field.isRequired}
                                    />
                                )}

                                {/* Connection Selector */}
                                {field.type === 'connectionSelector' && (
                                    <ConnectionSelector
                                        connectionTypes={step.connectionTypes}
                                        connectionSelected={connectionSelected}
                                        setConnectionSelected={setConnectionSelected}
                                        isRequired={field.isRequired}
                                    />
                                )}

                                {/* Traversal Selector */}
                                {field.type === 'traversalSelector' && <TraversalSelector />}

                                {/* Custom Actions */}
                                {field.type === 'customActionSelector' && (
                                    <CustomActionSelector
                                        actionSelection={actionSelection}
                                        setActionSelection={setActionSelection}
                                    />
                                )}

                                {/* Data Asset Selector */}
                                {field.type === 'dataAssetSelector' && (
                                    <DataAssetSelector
                                        dataAssetSelected={assetTypesSelected}
                                        setDataAssetSelected={setAssetTypesSelected}
                                    />
                                )}

                                {/* Condition Selector */}
                                {field.type === 'conditionSelector' && (
                                    <ConditionSelector
                                        selectedAssetTypes={assetTypesSelected}
                                        initialConditions={initialConditions}
                                        conditionSelection={conditionSelection}
                                        setConditionSelection={setConditionSelection}
                                    />
                                )}

                                {/* Category Selector */}
                                {field.type === 'categorySelector' && (
                                    <CategorySelector
                                        categorySelected={categorySelected}
                                        setCategorySelected={setCategorySelected}
                                        isRequired={field.isRequired}
                                    />
                                )}

                                {/* Text Input */}
                                {field.type === 'text' && (
                                    <Input
                                        type="text"
                                        value={field.label && data[field.label.toLowerCase()]}
                                        onChange={(e) =>
                                            setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })
                                        }
                                        required={field.isRequired}
                                    />
                                )}

                                {/* Text Area */}
                                {field.type === 'longtext' && (
                                    <Input.TextArea
                                        value={field.label && data[field.label.toLowerCase()]}
                                        onChange={(e) =>
                                            setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })
                                        }
                                        required={field.isRequired}
                                    />
                                )}
                            </StepField>
                        );
                    })}
                </Step>
            ))}
        </div>
    );
};
