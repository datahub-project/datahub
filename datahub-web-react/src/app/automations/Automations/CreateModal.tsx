import { Modal } from 'antd';
import React, { useState } from 'react';

import { useAutomationContext } from '@app/automations/Automations/AutomationProvider';
import {
    AutomationLogo,
    AutomationsDescription,
    AutomationsModalHeader,
    PremadeAutomationCard,
    PremadeAutomations,
} from '@app/automations/Automations/components';
import { useIsFormDisabled } from '@app/automations/Automations/hooks';
import { Configure } from '@app/automations/fields/configure';
import { templates } from '@app/automations/recipes';
import { AutomationTemplate } from '@app/automations/types';
import { getYaml } from '@app/automations/utils';
import { YamlEditor } from '@app/ingest/source/builder/YamlEditor';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { useAppConfig } from '@src/app/useAppConfig';

const SelectAutomationType = ({ setAutomation }: any) => {
    const { config } = useAppConfig();

    return (
        <PremadeAutomations>
            {templates.map((automation: AutomationTemplate) => {
                // Check if automation is disabled
                if (automation.isDisabled?.(config)) return null;
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
    const { type, typeTemplate, recipe, setAutomationType, setFormState, createAutomation } = useAutomationContext();
    const [showYaml, setShowYaml] = useState(false);

    // Check if the form is disabled
    const isDisabled = useIsFormDisabled(recipe);

    // Close the modal util
    const closeModal = () => {
        setIsOpen(false);
        setShowYaml(false);
        setFormState?.({});
    };

    // Handle going back to the automation selection
    const goBack = () => {
        setFormState?.({});
        setShowYaml(false);
    };

    // Handle form create submission
    const handleCreate = () => {
        createAutomation?.();
        closeModal();
    };

    // Conditional form details
    const formInfo = {
        modalTitle: 'Create an Automation',
        modalDescription: 'Configure an automation that takes action when important things occur',
        submitContent: 'Save & Run', // generic, gets updated based on recipe items
        submitFn: handleCreate,
    };

    const showPreselect = !type;
    const showForm = !showPreselect && !showYaml;

    return (
        <Modal
            title={
                typeTemplate ? (
                    <AutomationsModalHeader>
                        {typeTemplate?.logo && <AutomationLogo src={typeTemplate?.logo} alt={typeTemplate?.name} />}
                        <div>
                            <h2>{typeTemplate?.name}</h2>
                            <AutomationsDescription>{typeTemplate?.description}</AutomationsDescription>
                        </div>
                    </AutomationsModalHeader>
                ) : (
                    <AutomationsModalHeader>
                        <div>
                            <h2>{formInfo.modalTitle}</h2>
                            <AutomationsDescription>{formInfo.modalDescription}</AutomationsDescription>
                        </div>
                    </AutomationsModalHeader>
                )
            }
            footer={
                <ModalButtonContainer>
                    {showForm || (showYaml && <Button onClick={goBack}>Back</Button>)}
                    <Button color="gray" variant="text" onClick={closeModal}>
                        Cancel
                    </Button>
                    {type ? (
                        <Button onClick={formInfo.submitFn} disabled={isDisabled}>
                            {formInfo.submitContent}
                        </Button>
                    ) : null}
                </ModalButtonContainer>
            }
            onCancel={closeModal}
            open={isOpen}
            width={800}
        >
            {showPreselect && <SelectAutomationType setAutomation={setAutomationType} />}
            {showForm && <Configure />}
            {showYaml && <YamlEditor initialText={getYaml(recipe)} height="450px" onChange={() => null} isDisabled />}
        </Modal>
    );
};
