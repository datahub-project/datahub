/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ChildrenLoaderContextType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { DEFAULT_LOAD_BATCH_SIZE } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';

const DEFAULT_CONTEXT_STATE: ChildrenLoaderContextType = {
    get: () => undefined,
    upsert: () => {},
    onLoad: () => {},
    maxNumberOfChildrenToLoad: DEFAULT_LOAD_BATCH_SIZE,
};

const ChildrenLoaderContext = React.createContext<ChildrenLoaderContextType>(DEFAULT_CONTEXT_STATE);

export default ChildrenLoaderContext;
