import React from 'react';

import { PrimaryButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/PrimaryButton';

type Props = {
    title?: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const EditButton = ({ title = 'Edit', disabled, tooltip, onClick }: Props) => {
    return (
        <PrimaryButton
            title={title}
            disabled={disabled}
            tooltip={tooltip}
            onClick={onClick}
            dataTestId="edit-assertion-button"
        />
    );
};
