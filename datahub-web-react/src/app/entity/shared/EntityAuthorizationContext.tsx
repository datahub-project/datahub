import React, { useContext } from 'react';

export interface DataSetsPrivileges {
    editSchemaFieldDescription: boolean;
    editSchemaAddFieldDescription?: boolean;
    editSchemaFieldTags: boolean;
    editSchemaAddFieldTags: boolean;
    editSchemaFieldGlossaryTerms: boolean;
    editSchemaAddFieldGlossaryTerms: boolean;
}
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
    editSchemaAddFieldDescription: true,
    editSchemaFieldTags: true,
    editSchemaAddFieldTags: true,
    editSchemaFieldGlossaryTerms: true,
    editSchemaAddFieldGlossaryTerms: true,
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

export const useEntitySchemaPrivileges = () => {
    const {
        editSchemaFieldDescription,
        editSchemaAddFieldDescription,
        editSchemaFieldTags,
        editSchemaAddFieldTags,
        editSchemaFieldGlossaryTerms,
        editSchemaAddFieldGlossaryTerms,
    } = useContext(EntityAuthorizationContext);
    return {
        editSchemaFieldDescription,
        editSchemaAddFieldDescription,
        editSchemaFieldTags,
        editSchemaAddFieldTags,
        editSchemaFieldGlossaryTerms,
        editSchemaAddFieldGlossaryTerms,
    };
};
