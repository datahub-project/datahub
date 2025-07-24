import { useContext } from 'react';

import ChildrenLoaderContext from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderContext';

export function useChildrenLoaderContext() {
    return useContext(ChildrenLoaderContext);
}
