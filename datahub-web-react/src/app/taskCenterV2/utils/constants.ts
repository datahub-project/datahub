export const PROPOSALS_FILTER_LABELS = {
    createdBy: {
        displayName: 'Created By',
    },
    type: {
        displayName: 'Type',
    },
    status: {
        displayName: 'Status',
    },
    resource: {
        displayName: 'Entity',
    },
};

export const ACTION_REQUEST_DEFAULT_FACETS = ['createdBy', 'status', 'type', 'resource'];

// Facets to be displayed in the UI
export const ACTION_REQUEST_DISPLAY_FACETS = ['createdBy', 'status', 'type'];

export const ACTION_REQUEST_TYPE_TO_LABEL = {
    TAG_ASSOCIATION: 'Tag Association',
    TERM_ASSOCIATION: 'Term Association',
    CREATE_GLOSSARY_TERM: 'Create Glossary Term',
    CREATE_GLOSSARY_NODE: 'Create Glossary Node',
    UPDATE_DESCRIPTION: 'Update Description',
    DATA_CONTRACT: 'Data Contract',
    STRUCTURED_PROPERTY_ASSOCIATION: 'Structured Property Association',
    DOMAIN_ASSOCIATION: 'Domain Association',
    OWNER_ASSOCIATION: 'Owner Association',
};
