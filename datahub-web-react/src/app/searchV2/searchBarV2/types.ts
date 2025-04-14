import { EntityType } from '@src/types.generated';
import React from 'react';

export interface Option {
    value: string;
    label: React.ReactNode;
    type?: string | EntityType;
    style?: React.CSSProperties;
    disabled?: boolean;
}

export interface SectionOption extends Omit<Option, 'value'> {
    value?: string;
    options?: Option[];
}
