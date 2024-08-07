import React, { useContext, useState, ReactNode } from 'react';

import {
    useCreateActionPipelineMutation,
    useUpsertActionPipelineMutation,
    useDeleteActionPipelineMutation,
} from '@graphql/actionPipeline.generated';

import { AutomationTypes } from '@app/automations/constants';
import { FormDataType } from '@app/automations/types';

import { useAutomationsContext } from './AutomationsProvider';

export interface AutomationContextType {
    // Data from the parent
    urn?: string;
    type?: AutomationTypes;
    name?: string;
    category?: string;
    description?: string;
    definition?: any;
    localTemplate?: any;

    // Locally Set Data
    formData?: FormDataType;
    setFormData?: (data: any) => void;
    recipe?: any;
    setRecipe?: (data: any) => void;
    createAutomation?: (type: AutomationTypes) => void;
    updateAutomation?: () => void;
    deleteAutomation?: () => void;
}

export const AutomationContext = React.createContext<AutomationContextType>({
    urn: '',
    type: AutomationTypes.ACTION,
    name: undefined,
    category: undefined,
    description: undefined,
    localTemplate: {},
    definition: {},
    formData: {
        tagsAndTerms: { terms: [], tags: [], nodes: [] }, // This seems weird to me. Why have such a specific field here?
        connection: undefined,
        conditions: [],
        actions: [],
        source: [],
        name: undefined,
        description: undefined,
        category: undefined,
    },
    setFormData: () => {},
    recipe: {},
    setRecipe: () => {},
    createAutomation: () => null,
    updateAutomation: () => null,
    deleteAutomation: () => null,
});

export const useAutomationContext = () => useContext(AutomationContext);

interface Props {
    context?: AutomationContextType;
    children?: ReactNode | undefined;
}

export const AutomationContextProvider = ({ context, children }: Props) => {
    const [createActionPipelineMutation] = useCreateActionPipelineMutation();
    const [upsertActionPipelineMutation] = useUpsertActionPipelineMutation();
    const [deleteActionPipeline] = useDeleteActionPipelineMutation();

    // Overall context wrapper (manages the list of automations)
    const { refetchAutomations } = useAutomationsContext();

    // This is the form data that will be updated
    const [formData, setFormData] = useState<FormDataType>({
        tagsAndTerms: { terms: [], nodes: [], tags: [] },
        conditions: [],
        actions: [],
        source: [],
        connection: undefined,
        name: context?.name,
        description: context?.description,
        category: context?.category,
    });

    // This is the recipe that will be updated
    const [recipe, setRecipe] = useState<any>(context?.definition || {});

    // Create Automation Function
    const createAutomation = (type: AutomationTypes) => {
        // Create an ACTION automation
        if (type === AutomationTypes.ACTION) {
            createActionPipelineMutation({
                variables: {
                    input: {
                        name: formData.name || '',
                        category: formData.category || '',
                        description: formData.description,
                        type: recipe.action.type,
                        config: {
                            recipe: JSON.stringify(recipe),
                            version: undefined,
                            executorId: 'default',
                            debugMode: false,
                        },
                    },
                },
            }).finally(() => {
                setTimeout(() => refetchAutomations?.(), 2000);
            });
        }

        // Create a TEST automation
        if (context?.type === AutomationTypes.TEST) {
            // Create test
        }
    };

    // Update Automation Function
    const updateAutomation = () => {
        // Update an ACTION automation
        if (context?.type === AutomationTypes.ACTION) {
            upsertActionPipelineMutation({
                variables: {
                    urn: context?.urn || '',
                    input: {
                        name: formData.name || '',
                        category: formData.category || '',
                        description: formData.description,
                        type: recipe.action.type,
                        config: {
                            recipe: JSON.stringify(recipe),
                            version: undefined,
                            executorId: 'default',
                            debugMode: false,
                        },
                    },
                },
            }).then(() => refetchAutomations?.());
        }

        // Update a TEST automation
        if (context?.type === AutomationTypes.TEST) {
            // Update test
        }
    };

    // Delete Automation Function
    const deleteAutomation = () => {
        // Delete an ACTION automation
        if (context?.type === AutomationTypes.ACTION) {
            deleteActionPipeline({
                variables: {
                    urn: context?.urn || '',
                },
            }).then(() => refetchAutomations?.());
        }

        // Delete a TEST automation
        if (context?.type === AutomationTypes.TEST) {
            // Delete test
        }
    };

    return (
        <AutomationContext.Provider
            value={{
                ...context,
                formData,
                setFormData,
                recipe,
                setRecipe,
                createAutomation,
                updateAutomation,
                deleteAutomation,
            }}
        >
            {children}
        </AutomationContext.Provider>
    );
};
