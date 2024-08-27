import { Button, Input, Modal } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    onCloseModal: () => void;
    onOk?: (result: string) => void;
    title?: string;
    defaultValue?: string;
};

export const EditTextModal = ({ defaultValue, onCloseModal, onOk, title }: Props) => {
    const { t } = useTranslation();
    const [stagedValue, setStagedValue] = useState(defaultValue || '');
    return (
        <Modal
            title={title}
            visible
            onCancel={onCloseModal}
            keyboard
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button
                        data-testid="edit-text-done-btn"
                        disabled={stagedValue.trim().length === 0}
                        onClick={() => onOk?.(stagedValue)}
                    >
                        {t('common.done')}
                    </Button>
                </>
            }
        >
            <Input data-testid="edit-text-input" onChange={(e) => setStagedValue(e.target.value)} value={stagedValue} />
        </Modal>
    );
};
