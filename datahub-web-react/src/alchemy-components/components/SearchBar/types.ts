import React from 'react';

export interface SearchBarProps {
    placeholder?: string;
    value?: string;
    width?: string;
    height?: string;
    onChange?: (value: string, event: React.ChangeEvent<HTMLInputElement>) => void;
    allowClear?: boolean;
    disabled?: boolean;
    suffix?: React.ReactNode;
}
