import { message } from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import {
    DEFAULT_PLUGIN_FORM_STATE,
    PluginFormState,
    addCustomHeader,
    createFormStateFromPlugin,
    hasAdvancedSettings,
    removeCustomHeader,
    updateCustomHeader,
    updateFormField,
} from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { ValidationErrors, validatePluginForm } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormValidation';
import { extractPlatformFromUrl } from '@app/settingsV2/platform/ai/plugins/utils/pluginLogoUtils';
import {
    buildUpsertAiPluginInput,
    buildUpsertOAuthServerInput,
    extractOAuthServerIdFromUrn,
} from '@app/settingsV2/platform/ai/plugins/utils/pluginMutationBuilder';

import {
    useOAuthAuthorizationServerQuery,
    useServiceQuery,
    useUpsertAiPluginMutation,
    useUpsertOAuthAuthorizationServerMutation,
} from '@graphql/aiPlugins.generated';
import { AiPluginAuthType, AiPluginConfig } from '@types';

export interface UsePluginFormOptions {
    editingPlugin: AiPluginConfig | null;
    existingNames: string[];
    onSuccess: () => void;
    sourceConfig?: PluginSourceConfig;
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
    const { editingPlugin, existingNames, onSuccess, sourceConfig } = options;

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

    const [upsertAiPlugin, { loading: isSavingService }] = useUpsertAiPluginMutation();
    const [upsertOAuthServer, { loading: isSavingOAuth }] = useUpsertOAuthAuthorizationServerMutation();
    const isSaving = isSavingService || isSavingOAuth;

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
            // If editing with an existing OAuth server, update it separately first
            const hasExistingOAuth =
                isEditing && existingOAuthServerUrn && formState.authType === AiPluginAuthType.UserOauth;

            if (hasExistingOAuth) {
                const oauthInput = buildUpsertOAuthServerInput(formState, existingOAuthServerId!);
                await upsertOAuthServer({
                    variables: { input: oauthInput },
                });
            }

            // Build service input - use oauthServerUrn (not newOAuthServer) when editing existing OAuth
            const input = buildUpsertAiPluginInput(formState, {
                editingUrn,
                existingOAuthServerUrn: hasExistingOAuth ? existingOAuthServerUrn : null,
                sourceConfig,
            });

            const result = await upsertAiPlugin({
                variables: { input },
            });

            // Emit analytics event
            const pluginId = result.data?.upsertAiPlugin?.urn || editingUrn || undefined;
            const platform = extractPlatformFromUrl(formState.url);
            if (isEditing) {
                analytics.event({
                    type: EventType.UpdateAiPluginEvent,
                    pluginId: pluginId || '',
                    pluginType: 'MCP_SERVER',
                    authType: formState.authType,
                    displayName: formState.displayName,
                    platform,
                });
            } else {
                analytics.event({
                    type: EventType.CreateAiPluginEvent,
                    pluginId,
                    pluginType: 'MCP_SERVER',
                    authType: formState.authType,
                    displayName: formState.displayName,
                    platform,
                });
            }

            message.success(isEditing ? 'AI plugin updated successfully' : 'AI plugin created successfully');
            onSuccess();
        } catch (error) {
            message.error('Failed to save AI plugin');
            console.error('Error saving AI plugin:', error);
        }
    }, [
        formState,
        validationOptions,
        editingUrn,
        existingOAuthServerUrn,
        existingOAuthServerId,
        upsertAiPlugin,
        upsertOAuthServer,
        isEditing,
        onSuccess,
        sourceConfig,
    ]);

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
