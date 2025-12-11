/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Modal } from 'antd';
import React from 'react';

type Props = {
    cancelModalVisible?: boolean;
    onDiscard?: () => void;
    onCancel?: () => void;
};

export const DiscardDescriptionModal = ({ cancelModalVisible, onDiscard, onCancel }: Props) => {
    return (
        <>
            <Modal
                title="Exit Editor"
                visible={cancelModalVisible}
                destroyOnClose
                onCancel={onCancel}
                footer={[
                    <Button type="text" onClick={onCancel}>
                        Cancel
                    </Button>,
                    <Button onClick={onDiscard}>Yes</Button>,
                ]}
            >
                <p>Are you sure you want to close the documentation editor? Any unsaved changes will be lost.</p>
            </Modal>
        </>
    );
};
