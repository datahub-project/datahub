/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Input, Modal } from 'antd';
import React, { useState } from 'react';

type Props = {
    onCloseModal: () => void;
    onOk?: (result: string) => void;
    title?: string;
    defaultValue?: string;
};

export const EditTextModal = ({ defaultValue, onCloseModal, onOk, title }: Props) => {
    const [stagedValue, setStagedValue] = useState(defaultValue || '');
    return (
        <Modal
            title={title}
            open
            onCancel={onCloseModal}
            keyboard
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        data-testid="edit-text-done-btn"
                        disabled={stagedValue.trim().length === 0}
                        onClick={() => onOk?.(stagedValue)}
                    >
                        Done
                    </Button>
                </>
            }
        >
            <Input data-testid="edit-text-input" onChange={(e) => setStagedValue(e.target.value)} value={stagedValue} />
        </Modal>
    );
};
