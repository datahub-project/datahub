import React from 'react';

export interface ToggleCardProps {
    title: string;
    value: boolean;
    disabled?: boolean;
    subTitle?: React.ReactNode;
    children?: React.ReactNode;
    onToggle: (value: boolean) => void;
    toggleDataTestId?: string;
}
