import { Pill } from '@components';
import React from 'react';

import { FabricType } from '@types';

interface Props {
    environment: FabricType;
}

const EnvPill = ({ environment }: Props) => <Pill label={`${environment}`} size="sm" color="gray" clickable={false} />;

export default EnvPill;
