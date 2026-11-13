import { Button, Modal, Select } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

type Props = {
    onCloseModal: () => void;
    onOk?: (results: string[]) => void;
    title?: string;
    defaultValues?: string[];
};

const { Option } = Select;

export const ChooseEntityTypeModal = ({ defaultValues, onCloseModal, onOk, title }: Props) => {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const entityTypes = entityRegistry.getSearchEntityTypes();

    const [stagedValues, setStagedValues] = useState(defaultValues || []);

    const addEntityType = (newType) => {
        setStagedValues([...stagedValues, newType]);
    };

    const removeEntityType = (type) => {
        setStagedValues(stagedValues.filter((stagedValue) => stagedValue !== type));
    };

    return (
        <Modal
            title={title}
            open
            onCancel={onCloseModal}
            keyboard
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button disabled={stagedValues.length === 0} onClick={() => onOk?.(stagedValues)}>
                        {tc('done')}
                    </Button>
                </>
            }
        >
            <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder={t('chooseEntityType.placeholder')}
                onSelect={(newValue) => addEntityType(newValue)}
                onDeselect={(newValue) => removeEntityType(newValue)}
                value={stagedValues.map((stagedEntityType) => ({
                    value: stagedEntityType,
                    label: entityRegistry.getCollectionName(stagedEntityType as EntityType),
                }))}
                dropdownMatchSelectWidth={false}
            >
                {entityTypes.map((type) => (
                    <Option key={type} value={type}>
                        {entityRegistry.getCollectionName(type)}
                    </Option>
                ))}
            </Select>
        </Modal>
    );
};
