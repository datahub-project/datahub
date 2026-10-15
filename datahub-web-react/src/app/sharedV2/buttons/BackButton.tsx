import { Button, Tooltip } from '@components';
import { ArrowLeft } from '@phosphor-icons/react/dist/csr/ArrowLeft';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    onGoBack?: () => void;
}

export const BackButton = ({ onGoBack }: Props) => {
    const { t } = useTranslation('shared.misc');
    return (
        <Tooltip title={t('backButton.tooltip')} showArrow={false} placement="bottom">
            <Button onClick={onGoBack} variant="text" icon={{ icon: ArrowLeft, size: 'xl' }} size="xl" />
        </Tooltip>
    );
};
