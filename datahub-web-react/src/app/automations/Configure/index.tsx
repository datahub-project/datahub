/* eslint-disable */
// TODO: Cleanup linting errors

import React, { useState, useEffect, useRef } from 'react';

import { Input } from 'antd';

import { useAutomationContext } from '../Automations/AutomationProvider';
import { updateRecipe } from '../Automations/utils/updateRecipe';
import { getFields } from '@app/automations/utils';

import { FormDataType, Connection } from '../types';
import { Step, StepHeader, StepField } from './components';

import { TermSelector } from './fields/TermSelector';
import { ConnectionSelector } from './fields/ConnectionSelector';
import { CategorySelector } from './fields/CategorySelector';
import { TraversalSelector } from './fields/TraversalSelector';
import { CustomActionSelector } from './fields/CustomActionSelector';
import { DataAssetSelector } from './fields/DataAssetSelector';
import { ConditionSelector } from './fields/ConditionSelector';
import { TagTermToggle } from './fields/TagTermToggle';

interface Props {
    automation: any;
    isEdit?: boolean;
}

export const Configure = ({ automation, isEdit = false }: Props) => {
    const { formData, setFormData, recipe, setRecipe } = useAutomationContext();
    const steps = automation.fields || getFields(automation);
    const prevProps = useRef(formData);

    // Various field states
    const [actionSelection, setActionSelection] = useState<string[]>(formData?.actions || []);
    const [conditionSelection, setConditionSelection] = useState<string[]>([]);
    const [assetTypesSelected, setAssetTypesSelected] = useState<string[]>(formData?.source || []);
    const [tagsAndTermsSelected, setTagsAndTermsSelected] = useState<any>(
        formData?.tagsAndTerms || { terms: [], nodes: [], tags: [] },
    );
    const [termPropagationEnabled, setTermPropagationEnabled] = useState<boolean>(
        formData?.termPropagationEnabled || true,
    );
    const [tagPropagationEnabled, setTagPropagationEnabled] = useState<boolean>(
        formData?.tagPropagationEnabled || true,
    );
    const [connectionSelected, setConnectionSelected] = useState<Connection | undefined>(formData?.connection);
    const [categorySelected, setCategorySelected] = useState<string | undefined>(
        formData?.category || automation.category,
    );
    const [details, setDetails] = useState<any>({
        name: formData?.name || automation.name,
        description: formData?.description || automation.description,
    });

    // Form Data to be submitted
    const data: FormDataType = {
        tagsAndTerms: tagsAndTermsSelected,
        termPropagationEnabled,
        tagPropagationEnabled,
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
        if (hasChanged) {
            // Update the context
            setFormData?.(data);
            setRecipe?.(updateRecipe(recipe, data));
        }
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
                                {field.label && <label aria-required={field.isRequired}>{field.label}</label>}

                                {/* Term Selector */}
                                {field.type === 'termSelector' && (
                                    <TermSelector
                                        tagsAndTermsSelected={tagsAndTermsSelected}
                                        setTagsAndTermsSelected={setTagsAndTermsSelected}
                                        isRequired={field.isRequired}
                                        fieldTypes={field.fieldTypes}
                                        {...field.props}
                                    />
                                )}

                                {/* Propagation Type Selector */}
                                {field.type === 'tagTermToggle' && (
                                    <TagTermToggle
                                        tagPropagationEnabled={tagPropagationEnabled}
                                        setTagPropagationEnabled={setTagPropagationEnabled}
                                        termPropagationEnabled={termPropagationEnabled}
                                        setTermPropagationEnabled={setTermPropagationEnabled}
                                        {...field.props}
                                    />
                                )}

                                {/* Connection Selector */}
                                {field.type === 'connectionSelector' && (
                                    <ConnectionSelector
                                        connectionSelected={connectionSelected}
                                        setConnectionSelected={setConnectionSelected}
                                        isEdit={isEdit}
                                        {...field.props}
                                    />
                                )}

                                {/* Traversal Selector */}
                                {field.type === 'traversalSelector' && <TraversalSelector {...field.props} />}

                                {/* Custom Actions */}
                                {field.type === 'customActionSelector' && (
                                    <CustomActionSelector
                                        actionSelection={actionSelection}
                                        setActionSelection={setActionSelection}
                                        {...field.props}
                                    />
                                )}

                                {/* Data Asset Selector */}
                                {field.type === 'dataAssetSelector' && (
                                    <DataAssetSelector
                                        dataAssetSelected={assetTypesSelected}
                                        setDataAssetSelected={setAssetTypesSelected}
                                        {...field.props}
                                    />
                                )}

                                {/* Condition Selector */}
                                {field.type === 'conditionSelector' && (
                                    <ConditionSelector
                                        selectedAssetTypes={assetTypesSelected}
                                        conditionSelection={conditionSelection}
                                        setConditionSelection={setConditionSelection}
                                        {...field.props}
                                    />
                                )}

                                {/* Category Selector */}
                                {field.type === 'categorySelector' && (
                                    <CategorySelector
                                        categorySelected={categorySelected}
                                        setCategorySelected={setCategorySelected}
                                        isRequired={field.isRequired}
                                        {...field.props}
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
