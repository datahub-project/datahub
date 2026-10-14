import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Button as AntButton } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { Button } from '@src/alchemy-components';

interface Props {
    buttonProps?: Record<string, unknown>;
    buttonType?: string;
}

export const AddLinkModal = ({ buttonProps, buttonType }: Props) => {
    const { t } = useTranslation('entity.shared.components');
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
                    <Plus />
                    {t('links.addLink')}
                </Button>
            );
        }
        if (bType === 'text') {
            return (
                <AntButton data-testid="add-link-button" onClick={showModal} type="text">
                    <Plus />
                    {t('links.addLink')}
                </AntButton>
            );
        }
        return (
            <Button variant="outline" data-testid="add-link-button" onClick={showModal} {...buttonProps}>
                <Plus />
                {t('links.addLink')}
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
