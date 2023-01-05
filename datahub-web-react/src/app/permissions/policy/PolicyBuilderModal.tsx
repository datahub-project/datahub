import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Button, Modal, Steps } from 'antd';
import PolicyPrivilegeForm from './PolicyPrivilegeForm';
import PolicyTypeForm from './PolicyTypeForm';
import PolicyActorForm from './PolicyActorForm';
import { ActorFilter, Policy, PolicyType, ResourceFilter } from '../../../types.generated';
import { EMPTY_POLICY } from './policyUtils';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import ClickOutside from '../../shared/ClickOutside';

type Props = {
    policy: Omit<Policy, 'urn'>;
    setPolicy: (policy: Omit<Policy, 'urn'>) => void;
    visible: boolean;
    onClose: () => void;
    onSave: (savePolicy: Omit<Policy, 'urn'>) => void;
};

const StepsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const PrevButtonContainer = styled.div`
    display: flex;
    justify-content: start;
    margin-left: 12px;
`;

const NextButtonContainer = styled.div`
    display: flex;
    justify-content: end;
    margin-right: 12px;
`;

/**
 * Component used for constructing new policies. The purpose of this flow is to populate or edit a Policy
 * object through a sequence of steps.
 */
export default function PolicyBuilderModal({ policy, setPolicy, visible, onClose, onSave }: Props) {
    // Step control-flow.
    const [activeStepIndex, setActiveStepIndex] = useState(0);

    // Go to next step
    const next = () => {
        setActiveStepIndex(activeStepIndex + 1);
    };

    // Go to previous step
    const prev = () => {
        setActiveStepIndex(activeStepIndex - 1);
    };

    // Save or create a policy
    const onSavePolicy = () => {
        onSave(policy);
    };

    // Change the type of policy, either Metadata or Platform
    const setPolicyType = (type: PolicyType) => {
        // Important: If the policy type itself is changing, we need to clear policy state.
        if (type === PolicyType.Platform) {
            setPolicy({ ...policy, type, resources: EMPTY_POLICY.resources, privileges: [] });
        }
        setPolicy({ ...policy, type, privileges: [] });
    };

    // Step 1: Choose Policy Type
    const typeStep = () => {
        return {
            title: 'Choose Policy Type',
            content: (
                <PolicyTypeForm
                    policyType={policy.type}
                    setPolicyType={(type: PolicyType) => setPolicyType(type)}
                    policyName={policy.name}
                    setPolicyName={(name: string) => setPolicy({ ...policy, name })}
                    policyDescription={policy.description || ''}
                    setPolicyDescription={(description: string) => setPolicy({ ...policy, description })}
                />
            ),
            complete: policy.type && policy.name && policy.name.length > 0, // Whether the "next" button should appear.
        };
    };

    // Step 2: Select privileges step.
    const privilegeStep = () => ({
        title: 'Configure Privileges',
        content: (
            <PolicyPrivilegeForm
                policyType={policy.type}
                resources={policy.resources!}
                setResources={(resources: ResourceFilter) => {
                    setPolicy({ ...policy, resources });
                }}
                privileges={policy.privileges}
                setPrivileges={(privileges: string[]) => setPolicy({ ...policy, privileges })}
            />
        ),
        complete: policy.privileges && policy.privileges.length > 0, // Whether the "next" button should appear.
    });

    // Step 3: Assign Actors Step
    const actorStep = () => {
        return {
            title: 'Assign Users & Groups',
            content: (
                <PolicyActorForm
                    policyType={policy.type}
                    actors={policy.actors}
                    setActors={(actors: ActorFilter) =>
                        setPolicy({
                            ...policy,
                            actors,
                        })
                    }
                />
            ),
            complete: true, // Whether the "next" button should appear.
        };
    };

    // Construct final set of steps.
    const policySteps = [typeStep(), privilegeStep(), actorStep()];

    // Get active step.
    const activeStep = policySteps[activeStepIndex];

    // Whether we're editing or creating a policy.
    const isEditing = policy !== undefined && policy !== null;

    useEnterKeyListener({
        querySelectorToExecuteClick: '#nextButton',
    });

    useEnterKeyListener({
        querySelectorToExecuteClick: '#saveButton',
    });

    // modalClosePopup for outside policy modal click
    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit Policy Editor',
            content: `Are you sure you want to exit policy editor? All changes will be lost`,
            onOk() {
                onClose();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="PolicyBuilderModal">
            <Modal
                wrapClassName="PolicyBuilderModal"
                title={isEditing ? 'Edit a Policy' : 'Create a new Policy'}
                visible={visible}
                onCancel={onClose}
                closable
                width={750}
                footer={null}
            >
                <Steps current={activeStepIndex}>
                    {policySteps.map((item) => (
                        <Steps.Step key={item.title} title={item.title} />
                    ))}
                </Steps>
                <div className="steps-content">{activeStep.content}</div>
                <StepsContainer>
                    <PrevButtonContainer>
                        {activeStepIndex > 0 && <Button onClick={() => prev()}>Previous</Button>}
                    </PrevButtonContainer>
                    <NextButtonContainer>
                        {activeStepIndex < policySteps.length - 1 && activeStep.complete && (
                            <Button id="nextButton" type="primary" onClick={() => next()}>
                                Next
                            </Button>
                        )}
                        {activeStepIndex === policySteps.length - 1 && activeStep.complete && (
                            <Button id="saveButton" type="primary" onClick={onSavePolicy}>
                                Save
                            </Button>
                        )}
                    </NextButtonContainer>
                </StepsContainer>
            </Modal>
        </ClickOutside>
    );
}
