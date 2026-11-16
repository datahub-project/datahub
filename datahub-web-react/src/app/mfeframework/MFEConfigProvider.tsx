import React, { useMemo } from 'react';

import { MFEConfigContext, MFEConfigContextType } from '@app/mfeframework/MFEConfigContext';
import { useMFEConfigFetch } from '@app/mfeframework/mfeConfigLoader';

/**
 * Fetches /mfe/config once and shares the parsed schema with every consumer (navigation, routes,
 * entity-page slots). Mount once, high in the authenticated tree.
 */
export default function MFEConfigProvider({ children }: { children: React.ReactNode }) {
    const { config, loading } = useMFEConfigFetch();
    const value = useMemo<MFEConfigContextType>(() => ({ provided: true, config, loading }), [config, loading]);
    return <MFEConfigContext.Provider value={value}>{children}</MFEConfigContext.Provider>;
}
