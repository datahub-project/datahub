import { Pill } from '@components';
import React from 'react';

import { Maybe, PolicyState, PolicyType } from '@types';

export function PolicyTypePill({ type }: { type?: Maybe<PolicyType | string> }) {
    const label = (type?.charAt(0)?.toUpperCase() ?? '') + (type?.slice(1)?.toLowerCase() ?? '');
    const isPlatform = type?.toUpperCase() === 'PLATFORM';
    return <Pill label={label} color={isPlatform ? 'blue' : 'violet'} variant="filled" size="sm" clickable={false} />;
}

export function PolicyStatePill({ state }: { state?: Maybe<PolicyState | string> }) {
    const isActive = state === PolicyState.Active;
    return (
        <Pill
            label={(state?.charAt(0) ?? '') + (state?.slice(1)?.toLowerCase() ?? '')}
            color={isActive ? 'green' : 'red'}
            size="sm"
            clickable={false}
        />
    );
}
