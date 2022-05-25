import React, { useContext } from 'react';

// TODO: move the types into types.ts file
// export type CommonPrivileges = {
//     editOwners: boolean;
//     editDocumentation: boolean;
//     editGlossaryTerms: boolean;
//     editTags: boolean;
//     editDomain: boolean;
//     editLinks: boolean;
//     editDeprecation: boolean;
// };
// TODO: move the types into types.ts file
export interface DataSetsPrivileges {
    editSchemaFieldDescription: boolean;
    editSchemaFieldTags: boolean;
    editSchemaFieldGlossaryTerms: boolean;
}
// TODO: move the types into types.ts file
export interface EntityAuthorizationType extends DataSetsPrivileges {
    editOwners: boolean;
    editDocumentation: boolean;
    editGlossaryTerms: boolean;
    editTags: boolean;
    editDomain: boolean;
    editLinks: boolean;
    editDeprecation: boolean;
}

const EntityAuthorizationContext = React.createContext<EntityAuthorizationType>({
    // Common Privileges
    editOwners: true,
    editDocumentation: true,
    editGlossaryTerms: true,
    editTags: true,
    editDomain: true,
    editLinks: true,
    editDeprecation: true,
    // Dataset-only
    editSchemaFieldDescription: true,
    editSchemaFieldTags: true,
    editSchemaFieldGlossaryTerms: false,
});
// display name set to context
EntityAuthorizationContext.displayName = 'AuthorizationContext';

export default EntityAuthorizationContext;

export const useEntityCommonPrivileges = () => {
    const { editOwners, editDocumentation, editGlossaryTerms, editTags, editDomain, editLinks, editDeprecation } =
        useContext(EntityAuthorizationContext);
    return {
        editOwners,
        editDocumentation,
        editGlossaryTerms,
        editTags,
        editDomain,
        editLinks,
        editDeprecation,
    };
};
