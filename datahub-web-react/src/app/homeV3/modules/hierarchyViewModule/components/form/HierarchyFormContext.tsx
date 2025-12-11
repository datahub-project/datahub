/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
