import { Button, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    buttonLabel?: string;
    isButtonDisabled?: boolean;
    dataTestId?: string;
    onButtonClick?: () => void;
}

const AddButton = ({ buttonLabel, isButtonDisabled, dataTestId, onButtonClick }: Props) => {
    const { t } = useTranslation('entity.profile.queries');
    return (
        <Tooltip
            placement="right"
            title={
                (isButtonDisabled && t('queriesTab.addUnauthorizedMessage')) ||
                t('queriesTab.addHighlightedQueryTooltip')
            }
        >
            <Button
                disabled={isButtonDisabled}
                variant="outline"
                size="sm"
                icon={{ icon: Plus }}
                onClick={onButtonClick}
                data-testid={dataTestId}
            >
                {buttonLabel}
            </Button>
        </Tooltip>
    );
};

export default AddButton;
