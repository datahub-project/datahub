import { Pill } from '@components';
import React from 'react';

import { useAppConfig } from '@app/useAppConfig';

import { FabricType } from '@types';

/** True when the instance-wide toggle is on and the entity has a resolved environment. */
export function useShowEnvPill(environment?: FabricType | null): boolean {
    const appConfig = useAppConfig();
    return !!appConfig.config?.visualConfig?.showEnvironmentBadge && !!environment;
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
