import React from 'react';

import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';

export const MFEBaseConfigurablePage = ({ config }: { config: MFEConfig }) => {
    return <div>{config.label}</div>;
};
