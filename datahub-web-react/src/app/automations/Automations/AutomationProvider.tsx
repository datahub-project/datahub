/* eslint-disable no-param-reassign */
import React, { useContext, useState, useEffect, useMemo, ReactNode } from 'react';
import { message } from 'antd';
import { isEqual } from 'lodash';

import {
    useCreateActionPipelineMutation,
    useUpsertActionPipelineMutation,
    useDeleteActionPipelineMutation,
    useGetActionPipelineQuery,
} from '@graphql/actionPipeline.generated';

import { AutomationTypes } from '@app/automations/constants';
import { useAutomationsContext } from './AutomationsProvider';
import { updateFormData } from './utils/updateFormData';

import { openSuccessNotification, openErrorNotification } from './Notifications';

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
    formData?: any;
    setFormData?: (data: any) => void;
    recipe?: any;
    setRecipe?: (data: any) => void;
    createAutomation?: (type: AutomationTypes) => void;
    updateAutomation?: () => void;
    deleteAutomation?: () => void;

    // States
    isLoading?: boolean;
}

export const AutomationContext = React.createContext<AutomationContextType>({
    urn: '',
    type: AutomationTypes.ACTION,
    name: undefined,
    category: undefined,
    description: undefined,
    localTemplate: {},
    definition: {},
    formData: {},
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
    const { refetch } = useGetActionPipelineQuery({
        variables: {
            urn: context?.urn || '',
        },
    });

    const [createActionPipelineMutation] = useCreateActionPipelineMutation();
    const [upsertActionPipelineMutation] = useUpsertActionPipelineMutation();
    const [deleteActionPipeline] = useDeleteActionPipelineMutation();

    // Overall context wrapper (manages the list of automations)
    const { refetchAutomations } = useAutomationsContext();

    // This is the default recipe details
    const details = useMemo(
        () => ({
            name: context?.name || '',
            category: context?.category || '',
            description: context?.description || '',
        }),
        [context],
    );

    // This is the form data that will be updated
    const [formData, setFormData] = useState<any>({
        name: context?.name || '',
        category: context?.category || '',
        description: context?.description || '',
    });

    // This is the recipe that will be updated
    const [recipe, setRecipe] = useState<any>({
        ...details,
    });

    // Update default recipe based on the context
    useEffect(() => {
        const r = { ...context?.definition, ...details } || {};
        setRecipe(r);
        setFormData(updateFormData(r, details));
    }, [context, details]);

    // Update the form data when the recipe changes
    useEffect(() => {
        const updatedFormData = updateFormData(recipe, formData);
        if (!isEqual(updatedFormData, formData)) {
            setFormData(updatedFormData);
        }
    }, [recipe, formData]);

    // Clean recipe config of actionPipeline graph fields
    const cleanRecipe = (r: any) => {
        // Remove description & category
        delete r.description;
        delete r.category;
        delete r.executorId;
        return r;
    };

    // Create Automation Function
    const createAutomation = () => {
        message.loading({ content: 'Loading...', duration: 3 });
        createActionPipelineMutation({
            variables: {
                input: {
                    name: formData.name || '',
                    category: formData.category || '',
                    description: formData.description || '',
                    type: recipe.action.type,
                    config: {
                        recipe: JSON.stringify(cleanRecipe(recipe)),
                        version: undefined,
                        executorId: formData.executorId || recipe?.executorId || 'default',
                        debugMode: false,
                    },
                },
            },
        })
            .then(() => {
                refetchAutomations?.(true); // Fetch list of automations
                refetch?.(); // Fetch this automation
                openSuccessNotification('Automation succesfully created.');
            })
            .catch((error) => {
                openErrorNotification('Create Automation', error.message);
            });
    };

    // Update Automation Function
    const updateAutomation = () => {
        upsertActionPipelineMutation({
            variables: {
                urn: context?.urn || '',
                input: {
                    name: formData.name || '',
                    category: formData.category || '',
                    description: formData.description || '',
                    type: recipe.action.type,
                    config: {
                        recipe: JSON.stringify(cleanRecipe(recipe)),
                        version: undefined,
                        executorId: formData.executorId || 'default',
                        debugMode: false,
                    },
                },
            },
        })
            .then(() => {
                refetchAutomations?.(false); // Fetch list of automations
                refetch?.(); // Fetch this automation
                openSuccessNotification('Automation succesfully updated.');
            })
            .catch((error) => {
                openErrorNotification('Update Automation', error.message);
            });
    };

    // Delete Automation Function
    const deleteAutomation = () => {
        message.loading({ content: 'Loading...', duration: 3 });
        deleteActionPipeline({
            variables: {
                urn: context?.urn || '',
            },
        })
            .then(() => {
                refetchAutomations?.(true); // Fetch list of automations
                refetch?.(); // Fetch this automation
                openSuccessNotification('Automation succesfully deleted.');
            })
            .catch((error) => {
                openErrorNotification('Delete Automation', error.message);
            });
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
