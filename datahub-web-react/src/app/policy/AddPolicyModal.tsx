import React from 'react';
import { Button, Modal, Steps, message } from 'antd';
import MetadataPolicyPrivilegeForm from './MetadataPolicyPrivilegeForm';
import PlatformPolicyPrivilegeForm from './PlatformPolicyPrivilegeForm';
import PolicyTypeForm from './PolicyTypeForm';
import PolicyActorForm from './PolicyActorForm';

// Policy configurations.

type Props = {
    visible: boolean;
    onClose: () => void;
};

export default function AddPolicyModal({ visible, onClose }: Props) {
    const [currentStep, setCurrentStep] = React.useState(0);
    const [policyType, setPolicyType] = React.useState('Metadata'); // TODO: Return to PolicyType.METADATA

    const next = () => {
        setCurrentStep(currentStep + 1);
    };

    const prev = () => {
        setCurrentStep(currentStep - 1);
    };

    const getPrivilegeStep = (type) => {
        if (type === 'Metadata') {
            return <MetadataPolicyPrivilegeForm />;
        }
        return <PlatformPolicyPrivilegeForm />;
    };

    const typeStepView = <PolicyTypeForm selectPolicyType={(type: string) => setPolicyType(type)} />;
    const privilegeStepView = getPrivilegeStep(policyType);
    const actorStepView = <PolicyActorForm />;

    const buildPolicySteps = [
        {
            title: 'Choose Policy Type',
            content: typeStepView,
        },
        {
            title: 'Configure Privileges',
            content: privilegeStepView,
        },
        {
            title: 'Assign Users & Groups',
            content: actorStepView,
        },
    ];

    return (
        <Modal title="Create a new Policy" visible={visible} onCancel={onClose} closable width={750} footer={null}>
            <Steps current={currentStep}>
                {buildPolicySteps.map((item) => (
                    <Steps.Step key={item.title} title={item.title} />
                ))}
            </Steps>
            <div className="steps-content">{buildPolicySteps[currentStep].content}</div>
            <div className="steps-action">
                {currentStep < buildPolicySteps.length - 1 && (
                    <Button type="primary" onClick={() => next()}>
                        Next
                    </Button>
                )}
                {currentStep === buildPolicySteps.length - 1 && (
                    <Button type="primary" onClick={() => message.success('Processing complete!')}>
                        Done
                    </Button>
                )}
                {currentStep > 0 && (
                    <Button style={{ margin: '0 8px' }} onClick={() => prev()}>
                        Previous
                    </Button>
                )}
            </div>
        </Modal>
    );
}
