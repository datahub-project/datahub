import { SaveOutlined } from '@ant-design/icons';
import React from 'react';

import { PrimaryButton } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/details/PrimaryButton';

type Props = {
    title?: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const SaveButton = ({ title = 'Save', disabled, tooltip, onClick }: Props) => {
    return (
        <PrimaryButton icon={<SaveOutlined />} title={title} disabled={disabled} tooltip={tooltip} onClick={onClick} />
    );
};
