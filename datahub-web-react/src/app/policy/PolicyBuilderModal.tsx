import React, { useState } from 'react';
import { Button, Modal, Steps } from 'antd';
import PolicyPrivilegeForm from './PolicyPrivilegeForm';
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

// TODO: see if we can merge this into the step view information below.
// TODO: Figure out a way to pass in the completeStep function to step 0 and 3, with a persistent value returned.
// TODO: Rethink all of the useCallback going on in here. It does not seem very nice.
export default function PolicyBuilderModal({ policy, setPolicy, visible, onClose, onSave }: Props) {
    // Step control-flow.
    const [activeStepIndex, setActiveStepIndex] = useState(0);

    const next = () => {
        setActiveStepIndex(activeStepIndex + 1);
    };

    const prev = () => {
        setActiveStepIndex(activeStepIndex - 1);
    };

    const setPolicyType = (type: PolicyType) => {
        // Important. If the policy type itself is changing, we need to clear state.
        if (type === PolicyType.Platform) {
            setPolicy({ ...policy, type, resources: undefined });
        }
        setPolicy({ ...policy, type });
    };

    // Step 0.
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
            complete: policy.type && policy.name && policy.name.length > 0,
        };
    };

    const privilegeStepContent = () => {
        return (
            <PolicyPrivilegeForm
                policyType={policy.type}
                resources={policy.resources!}
                setResources={(resources: ResourceFilter) => setPolicy({ ...policy, resources })}
                privileges={policy.privileges}
                setPrivileges={(privileges: string[]) => setPolicy({ ...policy, privileges })}
            />
        );
    };

    // Step 1.
    const privilegeStep = () => ({
        title: 'Configure Privileges',
        content: privilegeStepContent(),
        complete: policy.privileges && policy.privileges.length > 0,
    });

    // Step 2.
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
            complete: true,
        };
    };

    // Construct final set of steps.
    const policySteps = [typeStep(), privilegeStep(), actorStep()];

    const onCreatePolicy = () => {
        onSave(policy);
    };

    const activeStep = policySteps[activeStepIndex];

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
                {activeStepIndex < policySteps.length - 1 && activeStep.complete && (
                    <Button type="primary" onClick={() => next()}>
                        Next
                    </Button>
                )}
                {activeStepIndex === policySteps.length - 1 && activeStep.complete && (
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
