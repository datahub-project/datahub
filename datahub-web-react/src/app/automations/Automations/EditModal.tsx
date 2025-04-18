import React, { useState } from 'react';

import { Modal } from 'antd';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { YamlEditor } from '../../ingest/source/builder/YamlEditor';

import { Configure } from '../fields/configure';
import { getYaml } from '../utils';

import { useAutomationContext } from './AutomationProvider';

import { AutomationsModalHeader, AutomationsDescription, AutomationLogo } from './components';
import { useIsFormDisabled } from './hooks';

type AutomationEditModalProps = {
    isOpen: boolean;
    setIsOpen: (open: boolean) => void;
};

export const AutomationEditModal = ({ isOpen, setIsOpen }: AutomationEditModalProps) => {
    const { recipe, typeTemplate, updateAutomation } = useAutomationContext();
    const [showYaml, setShowYaml] = useState(false);

    // Check if the form is disabled
    const isDisabled = useIsFormDisabled(recipe);

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

    // Conditional form details
    const formInfo = {
        modalTitle: 'Edit Automation',
        modalDescription: "Edit an existing automation's settings",
        submitContent: 'Save & Run',
        submitFn: handleUpdate,
    };

    // Form states
    const showForm = !showYaml;

    return (
        <Modal
            title={
                <AutomationsModalHeader>
                    {typeTemplate?.logo && <AutomationLogo src={typeTemplate?.logo} alt={typeTemplate?.name} />}
                    <div>
                        <h2>{formInfo.modalTitle}</h2>
                        <AutomationsDescription>{formInfo?.modalDescription}</AutomationsDescription>
                    </div>
                </AutomationsModalHeader>
            }
            footer={
                <ModalButtonContainer>
                    <Button color="gray" variant="text" onClick={closeModal}>
                        Cancel
                    </Button>
                    <Button onClick={formInfo.submitFn} disabled={isDisabled}>
                        {formInfo.submitContent}
                    </Button>
                </ModalButtonContainer>
            }
            onCancel={closeModal}
            open={isOpen}
            width={800}
        >
            {showForm && <Configure isEdit />}
            {showYaml && <YamlEditor initialText={getYaml(recipe)} height="450px" onChange={() => null} isDisabled />}
        </Modal>
    );
};
