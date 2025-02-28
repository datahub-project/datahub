/**
 * A persona type captured for the user.
 */
export enum PersonaType {
    TECHNICAL_USER = 'urn:li:dataHubPersona:technicalUser',
    BUSINESS_USER = 'urn:li:dataHubPersona:businessUser',
    DATA_STEWARD = 'urn:li:dataHubPersona:dataSteward',
    DATA_ENGINEER = 'urn:li:dataHubPersona:dataEngineer',
    DATA_LEADER = 'urn:li:dataHubPersona:dataLeader', // Deprecated symbol.
}

export const PERSONA_TYPE_TO_LABEL: Record<PersonaType, string> = {
    [PersonaType.TECHNICAL_USER]: 'Data Expert',
    [PersonaType.BUSINESS_USER]: 'Data User',
    [PersonaType.DATA_STEWARD]: 'Data Steward',
    [PersonaType.DATA_ENGINEER]: 'Data Engineer',
    [PersonaType.DATA_LEADER]: 'Data Leader', // Deprecated symbol.
};

export const PERSONA_TYPE_TO_VIEW_ICON = {
    [PersonaType.TECHNICAL_USER]: '💡',
    [PersonaType.BUSINESS_USER]: '📊',
    [PersonaType.DATA_STEWARD]: '📖',
    [PersonaType.DATA_ENGINEER]: '🔧',
};

/**
 * The default view names associated with each persona type
 */
export const PERSONA_TYPE_TO_VIEW_NAME = {
    [PersonaType.TECHNICAL_USER]: '💡  Data Expert',
    [PersonaType.DATA_ENGINEER]: '🔧  Data Engineer',
    [PersonaType.DATA_STEWARD]: '📖  Data Steward',
    [PersonaType.BUSINESS_USER]: '📊  Data User',
};

/**
 * The default view urns associated with each persona type
 */
export const PERSONA_TYPE_TO_VIEW_URN = {
    [PersonaType.TECHNICAL_USER]: 'urn:li:dataHubView:__default_technicalUser',
    [PersonaType.DATA_ENGINEER]: 'urn:li:dataHubView:__default_dataEngineer',
    [PersonaType.DATA_STEWARD]: 'urn:li:dataHubView:__default_dataSteward',
    [PersonaType.BUSINESS_USER]: 'urn:li:dataHubView:__default_businessUser',
};

export const PERSONA_TYPE_TO_DESCRIPTION: Record<PersonaType, string> = {
    [PersonaType.TECHNICAL_USER]: 'Explore tables, topics, databases, data pipelines, dashboards, and more',
    [PersonaType.BUSINESS_USER]: 'Explore tables, dashboards, and reports',
    [PersonaType.DATA_STEWARD]: 'Explore tables, data domains, data products, and data dictionaries',
    [PersonaType.DATA_ENGINEER]: 'Explore tables, topics, databases, and data pipelines',
    [PersonaType.DATA_LEADER]: 'Explore tables, topics, databases, data pipelines, dashboards, and more', // Deprecated symbol.
};

/**
 * Standard Persona Types To Display When User is Selecting.
 */
export const PERSONA_TYPES_TO_DISPLAY = [
    PersonaType.TECHNICAL_USER, // Data Expert
    PersonaType.DATA_ENGINEER, // Data Engineer
    PersonaType.DATA_STEWARD, // Data Steward
    PersonaType.BUSINESS_USER, // Data User
];

// For when we enabling fetching custom personas.
export const LOAD_CUSTOM_PERSONAS = false;

export const ROLE_TO_PERSONA_TYPE: Record<string, PersonaType> = {
    'Data Analyst': PersonaType.TECHNICAL_USER,
    'Data Engineer': PersonaType.TECHNICAL_USER,
    'Data Scientist': PersonaType.TECHNICAL_USER,
    'Business Intelligence (BI) Analyst': PersonaType.BUSINESS_USER,
    'ML Engineer': PersonaType.TECHNICAL_USER,
    'Software Engineer': PersonaType.TECHNICAL_USER,
    'Product Manager': PersonaType.BUSINESS_USER,
    'Data Steward': PersonaType.DATA_STEWARD,
    'Data Governance Lead': PersonaType.DATA_STEWARD,
    'Database Administrator': PersonaType.TECHNICAL_USER,
    'Head of Data': PersonaType.TECHNICAL_USER,
    'Head of Engineering': PersonaType.TECHNICAL_USER,
    'Marketing Manager': PersonaType.BUSINESS_USER,
    'Chief Executive Officer (CEO)': PersonaType.BUSINESS_USER,
    'Project Manager': PersonaType.BUSINESS_USER,
    'Legal Counsel / Compliance Officer': PersonaType.BUSINESS_USER,
    'Sales Manager': PersonaType.BUSINESS_USER,
    'Customer Success Manager': PersonaType.BUSINESS_USER,
    Other: PersonaType.BUSINESS_USER,
};
