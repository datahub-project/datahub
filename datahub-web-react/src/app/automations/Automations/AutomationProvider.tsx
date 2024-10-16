/* eslint-disable no-param-reassign */
import React, { useContext, useState, useEffect, useMemo, ReactNode, useCallback, useRef } from 'react';
import {
    useCreateActionPipelineMutation,
    useUpsertActionPipelineMutation,
    useDeleteActionPipelineMutation,
} from '@graphql/actionPipeline.generated';
import { ActionPipelineState, EntityType } from '@src/types.generated';
import { useApolloClient } from '@apollo/client';
import { mapRecipeToFormState } from './utils/mapRecipeToFormState';
import { openSuccessNotification, openErrorNotification } from './Notifications';
import { mapFormStateToRecipe } from './utils/mapFormStateToRecipe';
import { configMaps } from '../recipes';
import { getTemplate } from '../utils';
import { removeFromListAutomationsCache, updateListAutomationsCache } from './utils/cacheUtils';

export interface AutomationContextType {
    // For edits, we pass in the following information.
    urn?: string;
    initialName?: string;
    initialCategory?: string;
    initialDescription?: string;
    initialExecutorId?: string;
    initialRecipe?: any;

    // Locally Set Data
    type?: string;
    setAutomationType?: (newType: string) => void;
    typeTemplate?: any;
    formState?: any;
    setFormState?: (data: any) => void;
    recipe?: any;
    setRecipe?: (data: any) => void;
    createAutomation?: () => void;
    updateAutomation?: () => void;
    deleteAutomation?: () => void;

    // States
    isLoading?: boolean;
}

export const AutomationContext = React.createContext<AutomationContextType>({
    urn: '',
    type: '',
    setAutomationType: () => {},
    initialName: undefined,
    initialCategory: undefined,
    initialDescription: undefined,
    initialExecutorId: undefined,
    initialRecipe: {},
    formState: {},
    setFormState: () => {},
    recipe: {},
    setRecipe: () => {},
    createAutomation: () => null,
    updateAutomation: () => null,
    deleteAutomation: () => null,
});

export const useAutomationContext = () => useContext(AutomationContext);

// Important: Clean recipe config of top level fields present in the action pipeline, but not allowed in the Action Recipes themselves.
// If this is not handled properly, then the backend Pydantic models will throw an exception will unexpected fields!
const cleanRecipe = (r: any) => {
    delete r.description;
    delete r.category;
    delete r.executorId;
    return r;
};

const resolveVirtualFormStateFields = (formState: any): any => {
    if (!formState.type) {
        return formState;
    }

    const configMap = configMaps[formState.type];
    const virtualFormState = { ...formState };

    Object.keys(configMap).forEach((field) => {
        const fieldConfig = configMap[field];
        if (fieldConfig?.isVirtual) {
            const derivedValue = fieldConfig.resolveVirtualFormStateFieldValue(formState);
            virtualFormState[field] = derivedValue;
        }
    });

    return virtualFormState;
};

const resolveBaseFormStateFromVirtualFields = (newFormState: any, oldFormState: any) => {
    let resolvedFormState = { ...newFormState };

    if (!newFormState.type) {
        return newFormState;
    }

    const configMap = configMaps[newFormState.type];

    Object.keys(configMap).forEach((field) => {
        const fieldConfig = configMap[field];
        if (fieldConfig.isVirtual && newFormState[field] !== oldFormState[field]) {
            // If a virtual field has changed, reverse map it to the base form state
            const partialFormState = fieldConfig.onChangeVirtualFormStateFieldValue(newFormState[field]);
            resolvedFormState = { ...resolvedFormState, ...partialFormState };
        }
    });

    return resolvedFormState;
};

interface Props {
    context?: AutomationContextType;
    children?: ReactNode | undefined;
}

export const AutomationContextProvider = ({ context, children }: Props) => {
    const isInitialMount = useRef(true);

    const client = useApolloClient();
    const [createActionPipelineMutation] = useCreateActionPipelineMutation();
    const [upsertActionPipelineMutation] = useUpsertActionPipelineMutation();
    const [deleteActionPipeline] = useDeleteActionPipelineMutation();

    // This represents a global state for the automation form contents.
    const [formState, setFormState] = useState<Record<string, any>>({
        type: context?.type,
        name: context?.initialName,
        category: context?.initialCategory,
        description: context?.initialDescription,
        executorId: context?.initialExecutorId,
    });

    // This represents the object form of the JSON action recipe file.
    const [recipe, setRecipe] = useState<any>({
        ...context?.initialRecipe,
    });

    // This represents the "template" or "configuration" for a given automation type, or the set of predefined fields and placeholder text,
    // for a given automation type.
    const typeTemplate = useMemo(() => {
        return formState.type ? getTemplate(formState.type) : undefined;
    }, [formState.type]);

    const setFormStateFromRecipe = useCallback(
        (r: any) => {
            const updatedFormState = mapRecipeToFormState(r, formState);
            const finalFormState = resolveVirtualFormStateFields(updatedFormState);
            setFormState(finalFormState);
        },
        [formState],
    );

    // Keep the Recipe JSON in sync with the form state here.
    // Any time the form state is changed (e.g. by selecting values within a field component), we try to update the recipe accordingly.
    useEffect(() => {
        // Any time the form state changes, we map the form state to the recipe to keep
        // the recipe definition in sync with the form.
        const updatedRecipe = mapFormStateToRecipe(formState);
        setRecipe(updatedRecipe);
    }, [formState]);

    // If the initial recipe context changes, reset the form state.
    useEffect(() => {
        if (context?.initialRecipe && isInitialMount.current) {
            setFormStateFromRecipe(context?.initialRecipe);
            isInitialMount.current = false;
        }
    }, [context?.initialRecipe, setFormStateFromRecipe]);

    // Create Automation Function
    const createAutomation = () => {
        const newAutomationDetails = {
            type: formState.type || '',
            name: formState.name || '',
            category: formState.category || 'Data Discovery',
            description: formState.description || '',
            config: {
                recipe: JSON.stringify(cleanRecipe(recipe)),
                version: null,
                executorId: formState.executorId || 'default',
                debugMode: false,
            },
        };

        createActionPipelineMutation({
            variables: {
                input: newAutomationDetails,
            },
        })
            .then((result) => {
                const newAutomation = {
                    urn: result?.data?.createActionPipeline,
                    type: EntityType.ActionsPipeline,
                    status: 'RUNNING',
                    details: {
                        ...newAutomationDetails,
                        state: ActionPipelineState.Active,
                    },
                };
                updateListAutomationsCache(client, newAutomation, 100);
                openSuccessNotification('Automation succesfully created.');
            })
            .catch((error) => {
                openErrorNotification('Create Automation', error.message);
            });
    };

    // Update Automation Function
    const updateAutomation = () => {
        const updatedAutomationDetails = {
            type: formState.type,
            name: formState.name || '',
            category: formState.category || '',
            description: formState.description || '',
            config: {
                recipe: JSON.stringify(cleanRecipe(recipe)),
                version: null,
                executorId: formState.executorId || 'default',
                debugMode: false,
            },
        };
        upsertActionPipelineMutation({
            variables: {
                urn: context?.urn || '',
                input: updatedAutomationDetails,
            },
        })
            .then((result) => {
                const updatedAutomation = {
                    urn: result?.data?.upsertActionPipeline,
                    type: EntityType.ActionsPipeline,
                    status: 'RUNNING',
                    details: {
                        ...updatedAutomationDetails,
                        state: ActionPipelineState.Active,
                    },
                };
                updateListAutomationsCache(client, updatedAutomation, 100);
                openSuccessNotification('Automation succesfully updated.');
            })
            .catch((error) => {
                openErrorNotification('Update Automation', error.message);
            });
    };

    // Delete Automation Function
    const deleteAutomation = () => {
        deleteActionPipeline({
            variables: {
                urn: context?.urn || '',
            },
        })
            .then(() => {
                removeFromListAutomationsCache(client, context?.urn, 100);
                openSuccessNotification('Automation succesfully deleted.');
            })
            .catch((error) => {
                openErrorNotification('Delete Automation', error.message);
            });
    };

    /**
     * Called within children components to change the automation type being edited.
     */
    const onChangeAutomationType = (newType: string) => {
        // When changing the automation type, simply reset form state based on the default recipe provided
        // for the automation! (e.g. when creating a new automation)
        const partialFormState = { type: newType };
        const defaultRecipe = getTemplate(newType)?.defaultRecipe;
        const defaultRecipeFormState = defaultRecipe
            ? mapRecipeToFormState(defaultRecipe, partialFormState)
            : partialFormState;
        const resolvedFormState = resolveVirtualFormStateFields(defaultRecipeFormState);
        setFormState(resolvedFormState);
    };

    /**
     * Called within children components any time the global form state object should be updated,
     * e.g. any time a select or input value is changed.
     */
    const onSetFormState = (newFormState: any) => {
        // TODO: Ensure that form state gets updated with the proper defaults for a new recipe.
        const finalFormState = resolveBaseFormStateFromVirtualFields(newFormState, formState);
        setFormState(finalFormState);
    };

    return (
        <AutomationContext.Provider
            value={{
                ...context,
                type: formState.type, // Special type field is elevated.
                setAutomationType: onChangeAutomationType,
                typeTemplate,
                formState,
                setFormState: onSetFormState,
                recipe,
                setRecipe, // Careful about calling this directly, this does not update form state!
                createAutomation,
                updateAutomation,
                deleteAutomation,
            }}
        >
            {children}
        </AutomationContext.Provider>
    );
};
