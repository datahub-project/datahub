/**
 * A persona type captured for the user.
 */
export enum PersonaType {
    TECHNICAL_USER = 'urn:li:dataHubPersona:technicalUser',
    BUSINESS_USER = 'urn:li:dataHubPersona:businessUser',
    DATA_STEWARD = 'urn:li:dataHubPersona:dataSteward',
    DATA_LEADER = 'urn:li:dataHubPersona:dataLeader',
}

export const PERSONA_TYPE_TO_LABEL: Record<PersonaType, string> = {
    [PersonaType.TECHNICAL_USER]: 'Data Worker',
    [PersonaType.BUSINESS_USER]: 'Business User',
    [PersonaType.DATA_STEWARD]: 'Data Steward',
    [PersonaType.DATA_LEADER]: 'Data Leader',
};

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
