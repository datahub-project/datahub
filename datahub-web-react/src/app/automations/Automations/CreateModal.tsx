import React, { useState, useEffect, useMemo } from 'react';

import { Modal, Button } from 'antd';

import { useAppConfig } from '@src/app/useAppConfig';

import { YamlEditor } from '@app/ingest/source/builder/YamlEditor';
import { AutomationTypes } from '@app/automations/constants';
import { getYaml } from '@app/automations/utils';

import { Configure } from '../Configure';
import { automationTemplates } from '../automationTemplates';

import { useAutomationContext } from './AutomationProvider';

import {
    PremadeAutomations,
    PremadeAutomationCard,
    AutomationsModalHeader,
    AutomationModalFooter,
    AutomationsDescription,
    AutomationLogo,
} from './components';

const SelectPremadeAutomation = ({ setAutomation }: any) => {
    const {
        config: { classificationConfig },
    } = useAppConfig();
    const isSnowflakeEnabled = classificationConfig?.automations?.snowflake;

    return (
        <PremadeAutomations>
            {automationTemplates.map((automation: any) => {
                // Check if automation is disabled
                if (automation.isDisabled) return null;

                // Check if snowflake is enabled
                if (automation.platform === 'snowflake' && !isSnowflakeEnabled) return null;

                return (
                    <PremadeAutomationCard key={automation.key} onClick={() => setAutomation(automation.key)}>
                        {automation.logo && <AutomationLogo src={automation.logo} alt={automation.name} />}
                        <h2>{automation.name}</h2>
                        <AutomationsDescription>{automation.description}</AutomationsDescription>
                    </PremadeAutomationCard>
                );
            })}
        </PremadeAutomations>
    );
};

type AutomationCreateModalProps = {
    isOpen: boolean;
    setIsOpen: (isOpen: boolean) => void;
};

export const AutomationCreateModal = ({ isOpen, setIsOpen }: AutomationCreateModalProps) => {
    const { recipe, setRecipe, setFormData, createAutomation } = useAutomationContext();

    const [automation, setAutomation] = useState();
    const [showYaml, setShowYaml] = useState(false);

    // Get the automation info
    const template = useMemo(
        () => (automation ? automationTemplates.find((t) => t.key === automation) : undefined),
        [automation],
    );

    // Get the automation type (ACTION, TEST)
    const automationType = template ? (template?.type as AutomationTypes) : undefined;

    // Get the base recipe from the automation template
    const baseRecipe = useMemo(() => {
        if (!template) return {};
        return template.baseRecipe;
    }, [template]);

    // Update context formData on mount
    // This is necessary to ensure the form data is updated
    useEffect(() => {
        if (baseRecipe) setRecipe?.(baseRecipe);
    }, [baseRecipe, setRecipe]);

    // Check if the form is disabled
    const isDisabled = false;

    // Close the modal util
    const closeModal = () => {
        setIsOpen(false);
        setShowYaml(false);
        setFormData?.({});
        setAutomation(undefined);
    };

    // Handle going back to the automation selection
    const goBack = () => {
        setAutomation(undefined);
        setFormData?.({});
        setShowYaml(false);
    };

    // Handle form create submission
    const handleCreate = () => {
        if (!isDisabled) {
            // Create is handled by the context
            createAutomation?.(automationType || AutomationTypes.ACTION);
            closeModal();
        }
    };

    // Conditional form details
    const formInfo = {
        modalTitle: 'Create an Automation',
        modalDescription: 'Configure an automation that takes action when important things occur',
        submitContent: 'Save', // generic, gets updated based on recipe items
        submitFn: handleCreate,
    };

    if (automationType === AutomationTypes.ACTION) formInfo.submitContent = 'Save and Run';
    if (automationType === AutomationTypes.TEST) formInfo.submitContent = 'Save and Schedule';

    // Form states
    const showPreselect = !automation;
    const showForm = automation && !showYaml;

    return (
        <Modal
            title={
                automation ? (
                    <AutomationsModalHeader>
                        {template?.logo && <AutomationLogo src={template.logo} alt={template.name} />}
                        <div>
                            <h2>{template?.name}</h2>
                            <AutomationsDescription>{template?.description}</AutomationsDescription>
                        </div>
                    </AutomationsModalHeader>
                ) : (
                    <AutomationsModalHeader>
                        {template?.logo && <AutomationLogo src={template?.logo} alt={template?.name} />}
                        <div>
                            <h2>{formInfo.modalTitle}</h2>
                            <AutomationsDescription>{formInfo.modalDescription}</AutomationsDescription>
                        </div>
                    </AutomationsModalHeader>
                )
            }
            footer={
                <AutomationModalFooter>
                    <div>
                        {/* {(showForm || showYaml) && (
                            <YamlButtonsContainer>
                                <TextButton isActive={!showYaml} onClick={() => setShowYaml(!showYaml)}>
                                    <FormOutlined /> Form
                                </TextButton>
                                <TextButton isActive={showYaml} onClick={() => setShowYaml(!showYaml)}>
                                    <CodeOutlined /> YAML
                                </TextButton>
                            </YamlButtonsContainer>
                        )} */}
                    </div>
                    <div>
                        {showForm || (showYaml && <Button onClick={goBack}>Back</Button>)}
                        <Button onClick={closeModal}>Cancel</Button>
                        <Button type="primary" onClick={formInfo.submitFn} disabled={isDisabled}>
                            {formInfo.submitContent}
                        </Button>
                    </div>
                </AutomationModalFooter>
            }
            onCancel={closeModal}
            open={isOpen}
            width={800}
        >
            {showPreselect && <SelectPremadeAutomation setAutomation={setAutomation} />}
            {showForm && <Configure automation={template} />}
            {showYaml && <YamlEditor initialText={getYaml(recipe)} height="450px" onChange={() => null} isDisabled />}
        </Modal>
    );
};
