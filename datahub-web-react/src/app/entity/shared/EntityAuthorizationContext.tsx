import React, { useContext } from 'react';

// TODO: move the types into types.ts file
export type CommonPrivileges = {
    editOwners: boolean;
    editDocumentation: boolean;
    editGlossaryTerms: boolean;
    editTags: boolean;
    editDomain: boolean;
    editLinks: boolean;
    editDeprecation: boolean;
};
// TODO: move the types into types.ts file
export type DataSetsPrivileges = {
    editSchemaFieldDescription: boolean;
    editSchemaFieldTags: boolean;
    editSchemaFieldGlossaryTerms: boolean;
};
// TODO: move the types into types.ts file
export type EntityAuthorizationType = {
    commonPrivileges: CommonPrivileges;
    dataSets: DataSetsPrivileges;
};

const EntityAuthorizationContext = React.createContext({
    // Common Privileges
    commonPrivileges: {
        editOwners: true,
        editDocumentation: false,
        editGlossaryTerms: true,
        editTags: true,
        editDomain: false,
        editLinks: true,
        editDeprecation: false,
    },
    // Dataset-only
    dataSets: {
        editSchemaFieldDescription: true,
        editSchemaFieldTags: true,
        editSchemaFieldGlossaryTerms: false,
    },
});
// display name set to context
EntityAuthorizationContext.displayName = 'AuthorizationContext';

export default EntityAuthorizationContext;

export const useEntityCommonPrivileges = () => {
    const { commonPrivileges } = useContext(EntityAuthorizationContext);
    return { commonPrivileges };
};
