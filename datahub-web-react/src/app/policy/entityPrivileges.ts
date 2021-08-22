export const ALL_PRIVILEGES = {
    editSelfEntityOwner: 'EDIT_SELF_ENTITY_OWNER',
    addEntityTag: 'ADD_ENTITY_TAG',
    addEntityTerm: 'ADD_ENTITY_TERM',
    addEntityDocLink: 'ADD_ENTITY_DOC_LINK',
    manageEntityTags: 'MANAGE_ENTITY_TAGS',
    manageEntityOwners: 'MANAGE_ENTITY_OWNERS',
    manageEntityDocs: 'MANAGE_ENTITY_DOCS',
    manageEntityDocLinks: 'MANAGE_ENTITY_DOC_LINKS',
    manageEntityColDocs: 'MANAGE_ENTITY_COL_DOCS',
    manageEntityColTags: 'MANGAGE_ENTITY_COL_TAGS',
    manageEntity: 'MANAGE_ENTITY',
};

// Ideally this mapping would be pushed from the server.
export const ENTITY_TO_PRIVILEGES = {
    Dataset: ALL_PRIVILEGES,
    Chart: Object.values(ALL_PRIVILEGES).filter(
        (priv) => priv !== ALL_PRIVILEGES.manageEntityColDocs && priv !== ALL_PRIVILEGES.manageEntityColTags,
    ),
    Dashboard: Object.values(ALL_PRIVILEGES).filter(
        (priv) => priv !== ALL_PRIVILEGES.manageEntityColDocs && priv !== ALL_PRIVILEGES.manageEntityColTags,
    ),
    DataPipeline: Object.values(ALL_PRIVILEGES).filter(
        (priv) => priv !== ALL_PRIVILEGES.manageEntityColDocs && priv !== ALL_PRIVILEGES.manageEntityColTags,
    ),
    DataFlow: Object.values(ALL_PRIVILEGES).filter(
        (priv) => priv !== ALL_PRIVILEGES.manageEntityColDocs && priv !== ALL_PRIVILEGES.manageEntityColTags,
    ),
};
