import React from 'react';

import { AssetPropertiesContextType } from '@app/entityV2/summary/properties/types';

const DEFAULT_CONTEXT: AssetPropertiesContextType = {
    properties: [],

    replace: () => {},
    remove: () => {},
    add: () => {},
};

const AssetPropertiesContext = React.createContext<AssetPropertiesContextType>(DEFAULT_CONTEXT);

export default AssetPropertiesContext;
