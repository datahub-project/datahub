import React, { useState } from 'react';

import { Modal } from 'antd';
import { useAppConfig } from '@src/app/useAppConfig';
import { YamlEditor } from '@app/ingest/source/builder/YamlEditor';
import { getYaml } from '@app/automations/utils';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { Button } from '@src/alchemy-components';
import { Configure } from '../fields/configure';
import { templates } from '../recipes';
import { useAutomationContext } from './AutomationProvider';
import {
    PremadeAutomations,
    PremadeAutomationCard,
    AutomationsModalHeader,
    AutomationsDescription,
    AutomationLogo,
} from './components';

import { automationType as aiTermClassificationActionType } from '../recipes/glossaryTerm/glossaryTermAI';
import { useIsFormDisabled } from './hooks';

const SelectAutomationType = ({ setAutomation }: any) => {
    const {
        config: { classificationConfig },
    } = useAppConfig();
    const isSnowflakeEnabled = classificationConfig?.automations?.snowflake;
    const isAiTermsEnabled = classificationConfig?.automations?.aiTermClassification;

    return (
        <PremadeAutomations>
            {templates.map((automation: any) => {
                // Check if automation is disabled
                if (automation.isDisabled) return null;

                // Automation feature flags.
                // TODO: Add bigquery feature flag here.
                if (automation.platform === 'snowflake' && !isSnowflakeEnabled) return null;
                if (automation.type === aiTermClassificationActionType && !isAiTermsEnabled) return null;

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
