import React, { useCallback, useMemo, useState } from 'react';
import { Button, Modal, Steps, message } from 'antd';
import MetadataPolicyPrivilegeForm from './MetadataPolicyPrivilegeForm';
import PlatformPolicyPrivilegeForm from './PlatformPolicyPrivilegeForm';
import PolicyTypeForm from './PolicyTypeForm';
import PolicyActorForm from './PolicyActorForm';

type Props = {
    policy?: any;
    visible: boolean;
    onClose: () => void;
};

/**
 * Represents the state of a modal flow step.
 */
enum StepState {
    COMPLETE = 'COMPLETE',
    INCOMPLETE = 'INCOMPLETE',
}

// TODO: see if we can merge this into the step view information below.
// TODO: Figure out a way to pass in the completeStep function to step 0 and 3, with a persistent value returned.
// TODO: Rethink all of the useCallback going on in here. It does not seem very nice.
const INITIAL_STEP_STATES = [StepState.INCOMPLETE, StepState.INCOMPLETE, StepState.COMPLETE];

export default function PolicyBuilderModal({ policy, visible, onClose }: Props) {
    // Whether we're editing or creating a policy.
    const isEditing = policy !== undefined && policy !== null;

    // Step control-flow.
    const [stepStates, setStepStates] = useState(INITIAL_STEP_STATES);
    const [activeStepIndex, setActiveStepIndex] = useState(0);

    // Step data.
    const [policyType, setPolicyType] = useState('Metadata'); // TODO: Return to PolicyType.METADATA
    const [policyName, setPolicyName] = useState(isEditing ? policy.name : '');
    const [policyDescription, setPolicyDescription] = useState(isEditing ? policy.description : '');
    const [privileges, setPrivileges] = useState(isEditing ? policy.privileges : []);
    const [assetType, setAssetType] = useState(isEditing ? policy.resource.type : '');
    const [assetUrns, setAssetUrns] = useState(isEditing ? policy.resource.urns : []);
    const [appliesToOwners, setAppliesToOwners] = useState(isEditing ? policy.actors.appliesToOwners : false);
    const [groupActors, setGroupActors] = useState(isEditing ? policy.actors.groups : []);
    const [userActors, setUserActors] = useState(isEditing ? policy.actors.user : []);

    const next = () => {
        setActiveStepIndex(activeStepIndex + 1);
    };

    const prev = () => {
        setActiveStepIndex(activeStepIndex - 1);
    };

    const updateStepState = useCallback(
        (step: number, state: StepState) => {
            const newStepStates = [...stepStates];
            newStepStates[step] = state;
            setStepStates(newStepStates);
        },
        [stepStates],
    );

    const updateStepCompletion = useCallback(
        (step: number, isComplete: boolean) => {
            if (isComplete) {
                return updateStepState(step, StepState.COMPLETE);
            }
            return updateStepState(step, StepState.INCOMPLETE);
        },
        [updateStepState],
    );

    // Step 0.
    const typeStep = useCallback(
        (index: number) => {
            const updateStepComplete = (isComplete: boolean) => updateStepCompletion(index, isComplete);
            return {
                title: 'Choose Policy Type',
                content: (
                    <PolicyTypeForm
                        policyType={policyType}
                        setPolicyType={setPolicyType as any}
                        policyName={policyName}
                        setPolicyName={setPolicyName}
                        policyDescription={policyDescription}
                        setPolicyDescription={setPolicyDescription}
                        updateStepCompletion={updateStepComplete}
                    />
                ),
                state: stepStates[index],
            };
        },
        [policyType, policyName, policyDescription, stepStates, updateStepCompletion],
    );

    const privilegeStepContent = useCallback(
        (index: number) => {
            const updateStepComplete = (isComplete: boolean) => updateStepCompletion(index, isComplete);

            if (policyType === 'Metadata') {
                return (
                    <MetadataPolicyPrivilegeForm
                        privileges={privileges}
                        setPrivileges={setPrivileges as any}
                        assetType={assetType}
                        setAssetType={setAssetType as any}
                        assetUrns={assetUrns}
                        setAssetUrns={setAssetUrns as any}
                        updateStepCompletion={updateStepComplete}
                    />
                );
            }
            return (
                <PlatformPolicyPrivilegeForm
                    privileges={privileges}
                    setPrivileges={setPrivileges as any}
                    updateStepCompletion={(isComplete: boolean) => updateStepCompletion(index, isComplete)}
                />
            );
        },
        [policyType, privileges, assetType, assetUrns, updateStepCompletion],
    );

    // Step 1.
    const privilegeStep = useCallback(
        (index: number) => ({
            title: 'Configure Privileges',
            content: privilegeStepContent(index),
            state: stepStates[index],
        }),
        [privilegeStepContent, stepStates],
    );

    // Step 2.
    const actorStep = useCallback(
        (index: number) => ({
            title: 'Assign Users & Groups',
            content: (
                <PolicyActorForm
                    policyType={policyType}
                    appliesToOwners={appliesToOwners as any}
                    setAppliesToOwners={setAppliesToOwners as any}
                    userUrns={userActors}
                    setUserUrns={setUserActors as any}
                    groupUrns={groupActors}
                    setGroupUrns={setGroupActors as any}
                />
            ),
            state: stepStates[index],
        }),
        [policyType, appliesToOwners, userActors, groupActors, stepStates],
    );

    // Construct final set of steps.
    const policySteps = useMemo(
        () => [typeStep(0), privilegeStep(1), actorStep(2)],
        [typeStep, privilegeStep, actorStep],
    );

    const onCreatePolicy = () => {
        message.success('Successfully saved policy.');
        // TODO: Actually create a new policy. TODO: Validate the data.
        // TODO: Reset the state of the modal.
        onClose();
    };

    const activeStep = policySteps[activeStepIndex];

    const isStepComplete = (step: any) => {
        return step.state === StepState.COMPLETE;
    };

    return (
        <Modal
            title={isEditing ? 'Edit a policy' : 'Create a new Policy'}
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
            <div className="steps-action">
                {activeStepIndex < policySteps.length - 1 && isStepComplete(activeStep) && (
                    <Button type="primary" onClick={() => next()}>
                        Next
                    </Button>
                )}
                {activeStepIndex === policySteps.length - 1 && isStepComplete(activeStep) && (
                    <Button type="primary" onClick={onCreatePolicy}>
                        Save
                    </Button>
                )}
                {activeStepIndex > 0 && (
                    <Button style={{ margin: '0 8px' }} onClick={() => prev()}>
                        Previous
                    </Button>
                )}
            </div>
        </Modal>
    );
}
