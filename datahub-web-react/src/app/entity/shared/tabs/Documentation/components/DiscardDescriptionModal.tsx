import React, { useState } from 'react';
import { Modal, Button } from 'antd';
import { useRouteToTab } from '../../../EntityContext';

type Props = {
    buttonProps?: Record<string, unknown>;
    isDescriptionUpdated?: boolean;
    urn: string;
};

export const DiscardDescriptionModal = ({ buttonProps, isDescriptionUpdated, urn }: Props) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const routeToTab = useRouteToTab();
    const localStorageDictionary = localStorage.getItem('editedDescriptions');
    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};

    const showModal = () => {
        if (isDescriptionUpdated) {
            setIsModalVisible(true);
        } else {
            routeToTab({ tabName: 'Documentation' });
        }
    };

    const handleCancel = () => {
        setIsModalVisible(false);
    };

    const handleDiscard = () => {
        // Remove the urn_id from localStorage
        // Updating the localStorage after save
        delete editedDescriptions[urn];
        if (Object.keys(editedDescriptions).length === 0) {
            localStorage.removeItem('editedDescriptions');
        } else {
            localStorage.setItem('editedDescriptions', JSON.stringify(editedDescriptions));
        }
        routeToTab({ tabName: 'Documentation' });
    };

    return (
        <>
            <Button onClick={showModal} {...buttonProps}>
                Back
            </Button>
            <Modal
                title="Discard Changes"
                visible={isModalVisible}
                destroyOnClose
                onCancel={handleCancel}
                footer={[
                    <Button type="text" onClick={handleCancel}>
                        Cancel
                    </Button>,
                    <Button onClick={handleDiscard}>Discard</Button>,
                ]}
            >
                <p>Changes will not be saved. Do you want to proceed?</p>
            </Modal>
        </>
    );
};
