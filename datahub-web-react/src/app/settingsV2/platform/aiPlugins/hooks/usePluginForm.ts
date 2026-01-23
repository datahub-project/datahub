import { message } from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';

import {
    DEFAULT_PLUGIN_FORM_STATE,
    PluginFormState,
    addCustomHeader,
    createFormStateFromPlugin,
    hasAdvancedSettings,
    removeCustomHeader,
    updateCustomHeader,
    updateFormField,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginFormState';
import { ValidationErrors, validatePluginForm } from '@app/settingsV2/platform/aiPlugins/utils/pluginFormValidation';
import {
    buildUpsertServiceInput,
    extractOAuthServerIdFromUrn,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginMutationBuilder';

import {
    useOAuthAuthorizationServerQuery,
    useServiceQuery,
    useUpsertServiceMutation,
} from '@graphql/aiPlugins.generated';
import { AiPluginConfig } from '@types';

export interface UsePluginFormOptions {
    editingPlugin: AiPluginConfig | null;
    existingNames: string[];
    onSuccess: () => void;
}

export interface UsePluginFormResult {
    // Form state
    formState: PluginFormState;
    errors: ValidationErrors;
    showAdvanced: boolean;

    // Derived state
    isEditing: boolean;
    isDuplicating: boolean;
    isSaving: boolean;

    // Field update functions
    updateField: <K extends keyof PluginFormState>(field: K, value: PluginFormState[K]) => void;
    setShowAdvanced: (show: boolean) => void;

    // Custom header functions
    addHeader: () => void;
    updateHeader: (headerId: string, field: 'key' | 'value', value: string) => void;
    removeHeader: (headerId: string) => void;

    // Form actions
    handleSubmit: () => Promise<void>;
    getModalTitle: () => string;
}

/**
 * Hook for managing plugin form state and submission.
 * Follows the pattern from useWorkflowFormCompletion.
 */
export function usePluginForm(options: UsePluginFormOptions): UsePluginFormResult {
    const { editingPlugin, existingNames, onSuccess } = options;

    // Determine editing mode
    const editingUrn = editingPlugin?.service?.urn || null;
    const isEditing = !!editingUrn;
    const isDuplicating = !!editingPlugin && !editingUrn;

    // Get existing OAuth server URN if editing a plugin with OAuth
    const existingOAuthServerUrn = editingPlugin?.oauthConfig?.serverUrn || null;
    const existingOAuthServerId = extractOAuthServerIdFromUrn(existingOAuthServerUrn);

    // Form state
    const [formState, setFormState] = useState<PluginFormState>(DEFAULT_PLUGIN_FORM_STATE);
    const [errors, setErrors] = useState<ValidationErrors>({});
    const [showAdvanced, setShowAdvanced] = useState(false);

    // Fetch service details if editing
    const { data: serviceData } = useServiceQuery({
        variables: { urn: editingUrn! },
        skip: !editingUrn || !!editingPlugin?.service?.mcpServerProperties,
    });

    // Fetch OAuth server details if editing a plugin with OAuth
    const { data: oauthServerData } = useOAuthAuthorizationServerQuery({
        variables: { urn: existingOAuthServerUrn! },
        skip: !existingOAuthServerUrn,
    });

    const [upsertService, { loading: isSaving }] = useUpsertServiceMutation();

    // Populate form when editing or duplicating
    useEffect(() => {
        if (editingPlugin) {
            const newState = createFormStateFromPlugin(editingPlugin, serviceData, oauthServerData);
            setFormState(newState);
            if (hasAdvancedSettings(newState)) {
                setShowAdvanced(true);
            }
        }
    }, [editingPlugin, serviceData, oauthServerData]);

    // Validation options - memoized to prevent unnecessary re-renders
    const validationOptions = useMemo(
        () => ({
            isEditing,
            existingNames,
            originalName: isEditing ? editingPlugin?.service?.properties?.displayName : null,
        }),
        [isEditing, existingNames, editingPlugin?.service?.properties?.displayName],
    );

    // Field update function
    const updateField = useCallback(<K extends keyof PluginFormState>(field: K, value: PluginFormState[K]) => {
        setFormState((prev) => updateFormField(prev, field, value));
        // Clear error when field is updated
        setErrors((prev) => {
            const newErrors = { ...prev };
            delete newErrors[field];
            return newErrors;
        });
    }, []);

    // Custom header functions
    const addHeader = useCallback(() => {
        setFormState((prev) => addCustomHeader(prev));
    }, []);

    const updateHeader = useCallback((headerId: string, field: 'key' | 'value', value: string) => {
        setFormState((prev) => updateCustomHeader(prev, headerId, field, value));
    }, []);

    const removeHeader = useCallback((headerId: string) => {
        setFormState((prev) => removeCustomHeader(prev, headerId));
    }, []);

    // Form submission
    const handleSubmit = useCallback(async () => {
        const validationResult = validatePluginForm(formState, validationOptions);

        if (!validationResult.isValid) {
            setErrors(validationResult.errors);
            return;
        }

        try {
            const input = buildUpsertServiceInput(formState, {
                editingUrn,
                existingOAuthServerId,
            });

            await upsertService({
                variables: { input },
            });

            message.success(isEditing ? 'AI plugin updated successfully' : 'AI plugin created successfully');
            onSuccess();
        } catch (error) {
            message.error('Failed to save AI plugin');
            console.error('Error saving AI plugin:', error);
        }
    }, [formState, validationOptions, editingUrn, existingOAuthServerId, upsertService, isEditing, onSuccess]);

    // Modal title
    const getModalTitle = useCallback(() => {
        if (isEditing) return 'Edit AI Plugin';
        if (isDuplicating) return 'Duplicate AI Plugin';
        return 'Create AI Plugin';
    }, [isEditing, isDuplicating]);

    return {
        formState,
        errors,
        showAdvanced,
        isEditing,
        isDuplicating,
        isSaving,
        updateField,
        setShowAdvanced,
        addHeader,
        updateHeader,
        removeHeader,
        handleSubmit,
        getModalTitle,
    };
}
