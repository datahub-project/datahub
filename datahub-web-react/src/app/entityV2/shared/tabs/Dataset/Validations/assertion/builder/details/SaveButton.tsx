import React from 'react';

import { SaveOutlined } from '@ant-design/icons';
import { PrimaryButton } from './PrimaryButton';

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
