import { Pill, Tooltip } from '@components';
import React from 'react';

import { FabricType } from '@types';

interface Props {
    environment: FabricType;
}

const EnvPill = ({ environment }: Props) => (
    <Tooltip title={`Environment: ${environment}`} showArrow={false}>
        <span>
            <Pill label={`${environment}`} size="sm" color="gray" clickable={false} />
        </span>
    </Tooltip>
);

export default EnvPill;
