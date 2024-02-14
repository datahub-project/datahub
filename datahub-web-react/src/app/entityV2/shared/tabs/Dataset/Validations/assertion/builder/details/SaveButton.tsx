import React from 'react';

import { SaveOutlined } from '@ant-design/icons';
import { PrimaryButton } from './PrimaryButton';

type Props = {
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const SaveButton = ({ disabled, tooltip, onClick }: Props) => {
    return (
        <PrimaryButton icon={<SaveOutlined />} title="Save" disabled={disabled} tooltip={tooltip} onClick={onClick} />
    );
};
