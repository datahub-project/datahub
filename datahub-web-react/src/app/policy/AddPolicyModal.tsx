import React, { useMemo, useState } from 'react';
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
    const [currentStep, setCurrentStep] = useState(0);
    const [policyType, setPolicyType] = useState('Metadata'); // TODO: Return to PolicyType.METADATA
    const [privileges, setPrivileges] = useState([]);
    const [assetType, setAssetType] = useState('');
    const [assetUrns, setAssetUrns] = useState([]);
    // const [appliesToOwners, setAppliesToOwners] = useState([]);
    // const [actors, setActors] = useState([]);

    const next = () => {
        setCurrentStep(currentStep + 1);
    };

    const prev = () => {
        setCurrentStep(currentStep - 1);
    };

    const typeStepView = useMemo(
        () => <PolicyTypeForm policyType={policyType} setPolicyType={setPolicyType as any} />,
        [policyType],
    );
    const privilegeStepView = useMemo(() => {
        if (policyType === 'Metadata') {
            return (
                <MetadataPolicyPrivilegeForm
                    privileges={privileges}
                    setPrivileges={setPrivileges as any}
                    assetType={assetType}
                    setAssetType={setAssetType as any}
                    assetUrns={assetUrns}
                    setAssetUrns={setAssetUrns as any}
                />
            );
        }
        return <PlatformPolicyPrivilegeForm />;
    }, [policyType, privileges, assetType, assetUrns]);
    const actorStepView = useMemo(() => <PolicyActorForm />, []);

    const buildPolicySteps = useMemo(
        () => [
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
        ],
        [typeStepView, privilegeStepView, actorStepView],
    );

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
