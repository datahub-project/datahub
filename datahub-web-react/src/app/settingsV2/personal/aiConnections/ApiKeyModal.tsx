import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { Button, Input, Modal } from '@src/alchemy-components';

const FormFields = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const FooterButtons = styled.div`
    display: flex;
    gap: 8px;
    justify-content: flex-end;
    width: 100%;
`;

/**
 * An additional field to collect from the user alongside their API key.
 * Each field maps to a custom header that will be sent with plugin requests.
 */
export interface AdditionalApiKeyField {
    /** Custom header key this field maps to (e.g. "x-dbt-user-id") */
    key: string;
    /** Label shown to the user */
    label: string;
    /** Placeholder text */
    placeholder?: string;
    /** Helper text below the input */
    helperText?: string;
    /** Whether this field is required */
    required?: boolean;
}

interface ApiKeyModalProps {
    /** Whether the modal is visible */
    open: boolean;
    /** Plugin display name */
    pluginName: string;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Callback when API key is submitted (with optional additional headers) */
    onSubmit: (apiKey: string, additionalHeaders?: { key: string; value: string }[]) => Promise<void>;
    /** Optional additional fields to collect (e.g. dbt User ID) */
    additionalFields?: AdditionalApiKeyField[];
}

/**
 * Modal for entering a personal API key for an AI plugin.
 * Optionally collects additional fields that map to per-user custom headers.
 */
const ApiKeyModal: React.FC<ApiKeyModalProps> = ({ open, pluginName, onClose, onSubmit, additionalFields = [] }) => {
    const [apiKey, setApiKey] = useState('');
    const [additionalValues, setAdditionalValues] = useState<Record<string, string>>({});
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [errors, setErrors] = useState<Record<string, string>>({});

    // Reset state when modal opens/closes
    useEffect(() => {
        if (open) {
            setApiKey('');
            setAdditionalValues({});
            setErrors({});
            setIsSubmitting(false);
        }
    }, [open]);

    const validate = useCallback((): boolean => {
        const newErrors: Record<string, string> = {};

        if (!apiKey.trim()) {
            newErrors.apiKey = 'Please enter your API key';
        } else if (apiKey.trim().length < 8) {
            newErrors.apiKey = 'API key seems too short';
        }

        additionalFields.forEach((field) => {
            if (field.required && !additionalValues[field.key]?.trim()) {
                newErrors[field.key] = `${field.label} is required`;
            }
        });

        setErrors(newErrors);
        return Object.keys(newErrors).length === 0;
    }, [apiKey, additionalFields, additionalValues]);

    const handleSubmit = useCallback(async () => {
        if (!validate()) return;

        setIsSubmitting(true);
        try {
            const headers = additionalFields
                .filter((f) => additionalValues[f.key]?.trim())
                .map((f) => ({ key: f.key, value: additionalValues[f.key].trim() }));

            await onSubmit(apiKey.trim(), headers.length > 0 ? headers : undefined);
            onClose();
        } catch {
            // Error is handled by parent component
        } finally {
            setIsSubmitting(false);
        }
    }, [validate, apiKey, additionalFields, additionalValues, onSubmit, onClose]);

    const handleAdditionalValueChange = useCallback((key: string, value: string) => {
        setAdditionalValues((prev) => ({ ...prev, [key]: value }));
        setErrors((prev) => {
            const next = { ...prev };
            delete next[key];
            return next;
        });
    }, []);

    if (!open) return null;

    return (
        <Modal
            title={`Connect to ${pluginName}`}
            subtitle="Provide your API key to use this plugin with Ask DataHub."
            onCancel={onClose}
            width={480}
            footer={
                <FooterButtons>
                    <Button variant="outline" onClick={onClose}>
                        Cancel
                    </Button>
                    <Button variant="filled" onClick={handleSubmit} isLoading={isSubmitting}>
                        Connect
                    </Button>
                </FooterButtons>
            }
        >
            <FormFields>
                <Input
                    label="API Key"
                    isPassword
                    isRequired
                    placeholder="Enter your API key"
                    value={apiKey}
                    setValue={(val) => {
                        setApiKey(val);
                        setErrors((prev) => {
                            const next = { ...prev };
                            delete next.apiKey;
                            return next;
                        });
                    }}
                    error={errors.apiKey}
                />

                {additionalFields.map((field) => (
                    <Input
                        key={field.key}
                        label={field.label}
                        isRequired={field.required}
                        placeholder={field.placeholder}
                        helperText={field.helperText}
                        value={additionalValues[field.key] || ''}
                        setValue={(val) => handleAdditionalValueChange(field.key, val)}
                        error={errors[field.key]}
                    />
                ))}
            </FormFields>
        </Modal>
    );
};

export default ApiKeyModal;
