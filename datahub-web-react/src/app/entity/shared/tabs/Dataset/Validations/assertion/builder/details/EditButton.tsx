import React from 'react';

import { EditOutlined } from '@ant-design/icons';
import { PrimaryButton } from './PrimaryButton';

type Props = {
    title?: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const EditButton = ({ title = 'Edit', disabled, tooltip, onClick }: Props) => {
    return (
        <PrimaryButton icon={<EditOutlined />} title={title} disabled={disabled} tooltip={tooltip} onClick={onClick} />
    );
};
