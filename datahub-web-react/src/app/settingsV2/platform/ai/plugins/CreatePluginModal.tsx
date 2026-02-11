import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { usePluginForm } from '@app/settingsV2/platform/ai/plugins/hooks/usePluginForm';
import { PluginConfigurationStep } from '@app/settingsV2/platform/ai/plugins/sources/PluginConfigurationStep';
import { SourceSelectionStep } from '@app/settingsV2/platform/ai/plugins/sources/SourceSelectionStep';
import { detectPluginSourceName, getPluginSource } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources';
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { DEFAULT_PLUGIN_FORM_STATE } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { Button, Checkbox, Heading, Modal, Text } from '@src/alchemy-components';

import { AiPluginConfig } from '@types';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface CreatePluginModalProps {
    editingPlugin: AiPluginConfig | null;
    onClose: () => void;
    existingNames?: string[];
}

// ---------------------------------------------------------------------------
// Styled components
// ---------------------------------------------------------------------------

const ModalContent = styled.div`
    max-height: calc(100vh - 260px);
    overflow-y: auto;
    padding: 0 4px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
`;

const FooterButtons = styled.div`
    display: flex;
    gap: 8px;
`;

const CustomModalHeader = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const SubtitleRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
`;

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

enum ModalStep {
    SelectSource = 'SELECT_SOURCE',
    Configure = 'CONFIGURE',
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const CreatePluginModal: React.FC<CreatePluginModalProps> = ({ editingPlugin, onClose, existingNames = [] }) => {
    // Determine initial step:
    // - Editing/duplicating => skip straight to Configure
    // - Creating new => start at SelectSource
    const isEditingOrDuplicating = !!editingPlugin;

    // If editing, detect the source from the existing plugin
    const initialSourceName = isEditingOrDuplicating
        ? detectPluginSourceName(
              editingPlugin?.service?.mcpServerProperties?.url,
              editingPlugin?.service?.properties?.displayName,
          )
        : null;

    const [step, setStep] = useState<ModalStep>(isEditingOrDuplicating ? ModalStep.Configure : ModalStep.SelectSource);
    const [selectedSource, setSelectedSource] = useState<PluginSourceConfig | null>(
        initialSourceName ? getPluginSource(initialSourceName) : null,
    );

    // Hook up the form
    const {
        formState,
        errors,
        showAdvanced,
        isEditing,
        isSaving,
        updateField,
        setShowAdvanced,
        addHeader,
        updateHeader,
        removeHeader,
        handleSubmit,
        getModalTitle,
    } = usePluginForm({
        editingPlugin,
        existingNames,
        onSuccess: onClose,
        sourceConfig: selectedSource ?? undefined,
    });

    // When a source card is clicked, apply its defaults and move to Configure
    const handleSourceSelect = useCallback(
        (source: PluginSourceConfig) => {
            setSelectedSource(source);

            // Apply source defaults to the form
            const { defaults } = source;
            const keys = Object.keys(defaults) as (keyof typeof defaults)[];
            keys.forEach((key) => {
                const value = defaults[key];
                if (value !== undefined) {
                    updateField(key, value as any);
                }
            });

            setStep(ModalStep.Configure);
        },
        [updateField],
    );

    // Go back to source selection (reset form defaults)
    const handleBack = useCallback(() => {
        setSelectedSource(null);
        // Reset form to defaults
        const keys = Object.keys(DEFAULT_PLUGIN_FORM_STATE) as (keyof typeof DEFAULT_PLUGIN_FORM_STATE)[];
        keys.forEach((key) => {
            updateField(key, DEFAULT_PLUGIN_FORM_STATE[key] as any);
        });
        setStep(ModalStep.SelectSource);
    }, [updateField]);

    // Modal title text
    const titleText = useMemo(() => {
        if (isEditingOrDuplicating) return getModalTitle();
        if (step === ModalStep.SelectSource) return 'Create AI Plugin';
        return `Create ${selectedSource?.displayName ?? ''} Plugin`;
    }, [isEditingOrDuplicating, step, selectedSource, getModalTitle]);

    // Modal subtitle text
    const subtitleText = useMemo(() => {
        if (step === ModalStep.SelectSource) return undefined;
        return selectedSource?.configSubtitle ?? 'Add an MCP server that can be accessed by Ask DataHub';
    }, [step, selectedSource]);

    // When the source has a DataHub docs URL, render a custom header with an inline "Learn more" link.
    // Otherwise, pass plain strings and let the Modal render its default header.
    const hasDocs = selectedSource?.datahubDocsUrl && step === ModalStep.Configure;

    const title: React.ReactNode = useMemo(() => {
        if (!hasDocs) return titleText;

        return (
            <CustomModalHeader>
                <Heading type="h1" color="gray" colorLevel={600} weight="bold" size="lg">
                    {titleText}
                </Heading>
                <SubtitleRow>
                    <Text type="span" color="gray" colorLevel={1700} weight="medium">
                        {subtitleText}.
                    </Text>
                    <a href={selectedSource?.datahubDocsUrl} target="_blank" rel="noopener noreferrer">
                        <Button variant="text" color="violet" size="sm" style={{ paddingLeft: 0, paddingRight: 0 }}>
                            Learn More ↗
                        </Button>
                    </a>
                </SubtitleRow>
            </CustomModalHeader>
        );
    }, [hasDocs, titleText, subtitleText, selectedSource]);

    // Only pass subtitle when using the default string title (no docs link)
    const subtitle = hasDocs ? undefined : subtitleText;

    // Footer content changes depending on step
    const footer = useMemo(() => {
        if (step === ModalStep.SelectSource) {
            return null;
        }

        // Configuration step — enable toggle + back/cancel/save
        return (
            <ModalFooter>
                <Checkbox
                    label="Enable for Ask DataHub"
                    isChecked={formState.enabled}
                    setIsChecked={(checked) => updateField('enabled', checked)}
                    shouldHandleLabelClicks
                    data-testid="plugin-enabled-checkbox"
                />
                <FooterButtons>
                    {!isEditingOrDuplicating && (
                        <Button variant="secondary" onClick={handleBack} data-testid="plugin-back-button">
                            Back
                        </Button>
                    )}
                    <Button
                        variant="filled"
                        onClick={handleSubmit}
                        isLoading={isSaving}
                        data-testid="plugin-submit-button"
                    >
                        {isEditing ? 'Save' : 'Create'}
                    </Button>
                </FooterButtons>
            </ModalFooter>
        );
    }, [step, formState.enabled, isEditing, isEditingOrDuplicating, isSaving, handleBack, handleSubmit, updateField]);

    return (
        <Modal
            title={title}
            subtitle={subtitle}
            onCancel={onClose}
            width={step === ModalStep.SelectSource ? 640 : 600}
            maskClosable={false}
            data-testid="create-plugin-modal"
            footer={footer}
            wrapProps={{ style: { overflow: 'hidden' } }}
        >
            <ModalContent>
                {step === ModalStep.SelectSource && <SourceSelectionStep onSelect={handleSourceSelect} />}

                {step === ModalStep.Configure && selectedSource && (
                    <PluginConfigurationStep
                        sourceConfig={selectedSource}
                        formState={formState}
                        errors={errors}
                        isEditing={isEditing}
                        showAdvanced={showAdvanced}
                        setShowAdvanced={setShowAdvanced}
                        updateField={updateField}
                        addHeader={addHeader}
                        updateHeader={updateHeader}
                        removeHeader={removeHeader}
                    />
                )}
            </ModalContent>
        </Modal>
    );
};

export default CreatePluginModal;
