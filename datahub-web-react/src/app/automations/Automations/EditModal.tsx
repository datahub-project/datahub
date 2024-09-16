import React, { useState } from 'react';

import { Modal, Button } from 'antd';

import { YamlEditor } from '../../ingest/source/builder/YamlEditor';

import { Configure } from '../fields/configure';
import { AutomationTypes } from '../constants';
import { getYaml } from '../utils';

import { useAutomationContext } from './AutomationProvider';

import { AutomationsModalHeader, AutomationModalFooter, AutomationsDescription, AutomationLogo } from './components';

type AutomationEditModalProps = {
    isOpen: boolean;
    setIsOpen: (open: boolean) => void;
};

export const AutomationEditModal = ({ isOpen, setIsOpen }: AutomationEditModalProps) => {
    const { type, name, localTemplate, formData, recipe, updateAutomation } = useAutomationContext();
    const [showYaml, setShowYaml] = useState(false);

    // Check if the form is disabled
    const isDisabled = false;

    // Close the modal util
    const closeModal = () => {
        setIsOpen(false);
        setShowYaml(false);
    };

    // Handle form update submission
    const handleUpdate = () => {
        if (!isDisabled) {
            // Update is handled by the context
            updateAutomation?.();
            closeModal();
        }
    };

    const automation = {
        ...localTemplate,
        steps: localTemplate.steps || [], // failsafe
        name: formData?.name || name,
        baseRecipe: recipe,
        definition: recipe,
    };

    // Conditional form details
    const formInfo = {
        modalTitle: 'Edit Automation',
        modalDescription: "Edit an existing automation's settings",
        submitContent: 'Save', // generic, gets updated based on recipe items
        submitFn: handleUpdate,
    };

    if (type === AutomationTypes.ACTION) formInfo.submitContent = 'Save and Run';
    if (type === AutomationTypes.TEST) formInfo.submitContent = 'Save and Schedule';

    // Form states
    const showForm = !showYaml;

    return (
        <Modal
            title={
                <AutomationsModalHeader>
                    {localTemplate.logo && <AutomationLogo src={localTemplate.logo} alt={localTemplate.name} />}
                    <div>
                        <h2>{formInfo.modalTitle}</h2>
                        <AutomationsDescription>{formInfo?.modalDescription}</AutomationsDescription>
                    </div>
                </AutomationsModalHeader>
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
            {showForm && <Configure automation={automation} isEdit />}
            {showYaml && <YamlEditor initialText={getYaml(recipe)} height="450px" onChange={() => null} isDisabled />}
        </Modal>
    );
};
