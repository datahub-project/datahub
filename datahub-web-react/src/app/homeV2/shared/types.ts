/**
 * A persona type captured for the user.
 */
export enum PersonaType {
    TECHNICAL_USER = 'urn:li:dataHubPersona:technicalUser',
    BUSINESS_USER = 'urn:li:dataHubPersona:businessUser',
    DATA_STEWARD = 'urn:li:dataHubPersona:dataSteward',
    DATA_LEADER = 'urn:li:dataHubPersona:dataLeader',
    DATA_ENGINEER = 'urn:li:dataHubPersona:dataEngineer',
}

export const PERSONA_TYPE_TO_LABEL: Record<PersonaType, string> = {
    [PersonaType.TECHNICAL_USER]: 'Data Worker',
    [PersonaType.BUSINESS_USER]: 'Business User',
    [PersonaType.DATA_STEWARD]: 'Data Steward',
    [PersonaType.DATA_LEADER]: 'Data Leader',
    [PersonaType.DATA_ENGINEER]: 'Data Engineer',
};

export const PERSONA_TYPE_TO_DESCRIPTION: Record<PersonaType, string> = {
    [PersonaType.TECHNICAL_USER]:
        'Explore everything - tables, topics, databases, data pipelines, dashboards, and more',
    [PersonaType.BUSINESS_USER]: 'Explore tables, dashboards, and reports',
    [PersonaType.DATA_STEWARD]: 'Explore tables, data domains, data products, and data dictionaries',
    [PersonaType.DATA_LEADER]: 'Explore everything - tables, topics, databases, data pipelines, dashboards, and more', // should not be displayed.
    [PersonaType.DATA_ENGINEER]: 'Explore tables, topics, databases, and data pipelines',
};

/**
 * Standard Persona Types To Display When User is Selecting.
 */
export const PERSONA_TYPES_TO_DISPLAY = [
    PersonaType.TECHNICAL_USER, // Data Expert
    PersonaType.BUSINESS_USER, // Data User
    PersonaType.DATA_ENGINEER, // Data Engineer
    PersonaType.DATA_STEWARD, // Data Steward
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
    'Data Governance Lead': PersonaType.DATA_LEADER,
    'Database Administrator': PersonaType.TECHNICAL_USER,
    'Head of Data': PersonaType.DATA_LEADER,
    'Head of Engineering': PersonaType.DATA_LEADER,
    'Marketing Manager': PersonaType.BUSINESS_USER,
    'Chief Executive Officer (CEO)': PersonaType.BUSINESS_USER,
    'Project Manager': PersonaType.BUSINESS_USER,
    'Legal Counsel / Compliance Officer': PersonaType.BUSINESS_USER,
    'Sales Manager': PersonaType.BUSINESS_USER,
    'Customer Success Manager': PersonaType.BUSINESS_USER,
    Other: PersonaType.BUSINESS_USER,
};
