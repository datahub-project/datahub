import { Pill } from '@components';
import React from 'react';

import { PolicyState, PolicyType } from '@types';

export const PolicyTypePill = ({ type }: { type?: string | null }) => {
    const label = type ? type.charAt(0).toUpperCase() + type.slice(1).toLowerCase() : '';
    return (
        <Pill
            label={label}
            color={type?.toUpperCase() === PolicyType.Platform ? 'blue' : 'violet'}
            variant="filled"
            size="sm"
            clickable={false}
        />
    );
};

export const PolicyStatePill = ({ state }: { state?: string | null }) => (
    <Pill
        label={state ? state.charAt(0) + state.slice(1).toLowerCase() : ''}
        color={state === PolicyState.Active ? 'green' : 'red'}
        size="sm"
        clickable={false}
    />
);
