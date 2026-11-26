import React, { useContext } from 'react';

import { SchemaContextType } from '@app/entityV2/shared/types';

const SchemaContext = React.createContext<SchemaContextType>({
    refetch: () => Promise.resolve({}),
});

export default SchemaContext;

export const useSchemaRefetch = () => {
    const { refetch } = useContext(SchemaContext);
    return refetch;
};
