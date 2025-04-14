import React from 'react';

export interface SearchBarProps {
    placeholder?: string;
    value?: string;
    width?: string;
    onChange?: (value: string, event: React.ChangeEvent<HTMLInputElement>) => void;
    allowClear?: boolean;
    disabled?: boolean;
    suffix?: React.ReactNode;
}
