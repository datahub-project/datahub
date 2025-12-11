/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ReloadableContext } from '@app/sharedV2/reloadableContext/ReloadableContext';
import { ReloadableContextType } from '@app/sharedV2/reloadableContext/types';

export function useReloadableContext() {
    return React.useContext<ReloadableContextType>(ReloadableContext);
}
