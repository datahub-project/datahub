import { Pill } from '@components';
import React from 'react';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';

import { FabricType } from '@types';

/** True when the instance-wide toggle is on and the entity has a resolved environment. */
export function useShowEnvPill(environment?: FabricType | null): boolean {
    const { globalSettings } = useGlobalSettingsContext();
    return !!globalSettings?.visualSettings?.showEnvironmentBadge && !!environment;
}

interface Props {
    environment?: FabricType | null;
}

const EnvPill = ({ environment }: Props) => {
    const show = useShowEnvPill(environment);
    if (!show || !environment) return null;
    return <Pill label={environment.replace('_', ' ')} size="sm" color="gray" clickable={false} />;
};

export default EnvPill;
