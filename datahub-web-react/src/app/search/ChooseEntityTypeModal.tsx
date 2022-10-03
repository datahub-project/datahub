import { Button, Modal, Select } from 'antd';
import React, { useState } from 'react';
import { useEntityRegistry } from '../useEntityRegistry';

type Props = {
    onCloseModal: () => void;
    onOk?: (result: string) => void;
    title?: string;
    defaultValue?: string;
};

const { Option } = Select;

export const ChooseEntityTypeModal = ({ defaultValue, onCloseModal, onOk, title }: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityTypes = entityRegistry.getSearchEntityTypes();

    const [stagedValue, setStagedValue] = useState(defaultValue || entityTypes[0]);

    return (
        <Modal
            title={title}
            visible
            onCancel={onCloseModal}
            keyboard
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button disabled={stagedValue.length === 0} onClick={() => onOk?.(stagedValue)}>
                        Done
                    </Button>
                </>
            }
        >
            <Select
                onChange={(newValue) => setStagedValue(newValue)}
                value={stagedValue}
                dropdownMatchSelectWidth={false}
            >
                {entityTypes.map((type) => (
                    <Option value={type}>{entityRegistry.getCollectionName(type)}</Option>
                ))}
            </Select>
        </Modal>
    );
};
