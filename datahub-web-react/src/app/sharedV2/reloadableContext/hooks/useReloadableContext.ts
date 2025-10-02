import React from 'react';
import { ReloadableContext } from '@app/sharedV2/reloadableContext/ReloadableContext';
import { ReloadableContextType } from '@app/sharedV2/reloadableContext/types';

export function useReloadableContext() {
    return React.useContext<ReloadableContextType>(ReloadableContext);
}
