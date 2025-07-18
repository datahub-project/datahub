import React from 'react';

import { ChildrenLoaderContextType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';

const DEFAULT_CONTEXT_STATE: ChildrenLoaderContextType = {
    get: () => undefined,
    upsert: () => {},
    onLoad: () => {},
    maxNumberOfChildrenToLoad: DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD,
};

const ChildrenLoaderContext = React.createContext<ChildrenLoaderContextType>(DEFAULT_CONTEXT_STATE);

export default ChildrenLoaderContext;
