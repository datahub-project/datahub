import React from 'react';

import { HierarchyForm, HierarchyFormContextType } from '@app/homeV3/modules/hierarchyViewModule/components/form/types';
import { ASSET_TYPE_DOMAINS } from '@app/homeV3/modules/hierarchyViewModule/constants';

const DEFAULT_CONTEXT: HierarchyFormContextType = {
    initialValues: {
        name: '',
        assetsType: ASSET_TYPE_DOMAINS,
        domainAssets: [],
        glossaryAssets: [],
        showRelatedEntities: false,
    },
};

const HierarchyFormContext = React.createContext<HierarchyFormContextType>(DEFAULT_CONTEXT);

interface Props {
    initialValues: HierarchyForm;
}

export function useHierarchyFormContext() {
    return React.useContext(HierarchyFormContext);
}

export function HierarchyFormContextProvider({ children, initialValues }: React.PropsWithChildren<Props>) {
    return <HierarchyFormContext.Provider value={{ initialValues }}>{children}</HierarchyFormContext.Provider>;
}
