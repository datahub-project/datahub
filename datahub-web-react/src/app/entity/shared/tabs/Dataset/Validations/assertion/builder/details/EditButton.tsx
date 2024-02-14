import React from 'react';

import { EditOutlined } from '@ant-design/icons';
import { PrimaryButton } from './PrimaryButton';

type Props = {
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const EditButton = ({ disabled, tooltip, onClick }: Props) => {
    return (
        <PrimaryButton icon={<EditOutlined />} title="Edit" disabled={disabled} tooltip={tooltip} onClick={onClick} />
    );
};
