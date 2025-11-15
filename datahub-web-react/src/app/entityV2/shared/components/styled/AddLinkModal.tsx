import { PlusOutlined } from '@ant-design/icons';
import { Button as AntButton } from 'antd';
import React, { useState } from 'react';

import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { Button } from '@src/alchemy-components';

interface Props {
    buttonProps?: Record<string, unknown>;
    buttonType?: string;
}

export const AddLinkModal = ({ buttonProps, buttonType }: Props) => {
    const [isModalVisible, setIsModalVisible] = useState(false);

    const showModal = () => {
        setIsModalVisible(true);
    };

    const handleClose = () => {
        setIsModalVisible(false);
    };

    const renderButton = (bType: string | undefined) => {
        if (bType === 'transparent') {
            return (
                <Button data-testid="add-link-button" variant="outline" onClick={showModal} {...buttonProps}>
                    <PlusOutlined />
                    Add Link
                </Button>
            );
        }
        if (bType === 'text') {
            return (
                <AntButton data-testid="add-link-button" onClick={showModal} type="text">
                    <PlusOutlined />
                    Add Link
                </AntButton>
            );
        }
        return (
            <Button variant="outline" data-testid="add-link-button" onClick={showModal} {...buttonProps}>
                <PlusOutlined />
                Add Link
            </Button>
        );
    };

    return (
        <>
            {renderButton(buttonType)}
            {isModalVisible && <AddLinkModalUpdated setShowAddLinkModal={handleClose} />}
        </>
    );
};
