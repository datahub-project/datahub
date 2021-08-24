import React, { useCallback, useMemo, useState } from 'react';
import { Button, Modal, Steps } from 'antd';
import MetadataPolicyPrivilegeForm from './MetadataPolicyPrivilegeForm';
import PlatformPolicyPrivilegeForm from './PlatformPolicyPrivilegeForm';
import PolicyTypeForm from './PolicyTypeForm';
import PolicyActorForm from './PolicyActorForm';
import { ActorFilter, Policy, PolicyType, ResourceFilter } from '../../types.generated';

type Props = {
    policy: Omit<Policy, 'urn'>;
    setPolicy: (policy: Omit<Policy, 'urn'>) => void;
    visible: boolean;
    onClose: () => void;
    onSave: (savePolicy: Omit<Policy, 'urn'>) => void;
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

export default function PolicyBuilderModal({ policy, setPolicy, visible, onClose, onSave }: Props) {
    // Step control-flow.
    const [stepStates, setStepStates] = useState(INITIAL_STEP_STATES);
    const [activeStepIndex, setActiveStepIndex] = useState(0);

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
                        policyType={policy.type}
                        setPolicyType={(type: PolicyType) => setPolicy({ ...policy, type })}
                        policyName={policy.name}
                        setPolicyName={(name: string) => setPolicy({ ...policy, name })}
                        policyDescription={policy.description || ''}
                        setPolicyDescription={(description: string) => setPolicy({ ...policy, description })}
                        updateStepCompletion={updateStepComplete}
                    />
                ),
                state: stepStates[index],
            };
        },
        [policy, setPolicy, stepStates, updateStepCompletion],
    );

    const privilegeStepContent = useCallback(
        (index: number) => {
            const updateStepComplete = (isComplete: boolean) => updateStepCompletion(index, isComplete);

            if (policy.type === PolicyType.Metadata) {
                return (
                    <MetadataPolicyPrivilegeForm
                        resources={policy.resources!}
                        setResources={(resources: ResourceFilter) => setPolicy({ ...policy, resources })}
                        privileges={policy.privileges}
                        setPrivileges={(privileges: string[]) => setPolicy({ ...policy, privileges })}
                        updateStepCompletion={updateStepComplete}
                    />
                );
            }
            return (
                <PlatformPolicyPrivilegeForm
                    privileges={policy.privileges}
                    setPrivileges={(privileges: string[]) =>
                        setPolicy({
                            ...policy,
                            privileges,
                        })
                    }
                    updateStepCompletion={(isComplete: boolean) => updateStepCompletion(index, isComplete)}
                />
            );
        },
        [policy, setPolicy, updateStepCompletion],
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
            state: stepStates[index],
        }),
        [policy, setPolicy, stepStates],
    );

    // Construct final set of steps.
    const policySteps = useMemo(
        () => [typeStep(0), privilegeStep(1), actorStep(2)],
        [typeStep, privilegeStep, actorStep],
    );

    const onCreatePolicy = () => {
        onSave(policy);
    };

    const activeStep = policySteps[activeStepIndex];

    const isStepComplete = (step: any) => {
        return step.state === StepState.COMPLETE;
    };

    // Whether we're editing or creating a policy.
    const isEditing = policy !== undefined && policy !== null;

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
