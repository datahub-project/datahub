import React from 'react';

import { PrimaryButton } from './PrimaryButton';

type Props = {
    title?: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
};

export const SaveButton = ({ title = 'Save', disabled, tooltip, onClick }: Props) => {
    return <PrimaryButton title={title} disabled={disabled} tooltip={tooltip} onClick={onClick} />;
};
