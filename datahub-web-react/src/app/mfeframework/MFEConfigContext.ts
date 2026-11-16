import { createContext } from 'react';

import { MFESchema } from '@app/mfeframework/mfeConfigLoader';

export type MFEConfigContextType = {
    /** False when no provider is mounted above the consumer (tests, isolated renders). */
    provided: boolean;
    config: MFESchema | null;
    loading: boolean;
};

export const MFE_CONFIG_CONTEXT_DEFAULT: MFEConfigContextType = { provided: false, config: null, loading: false };

export const MFEConfigContext = createContext<MFEConfigContextType>(MFE_CONFIG_CONTEXT_DEFAULT);
