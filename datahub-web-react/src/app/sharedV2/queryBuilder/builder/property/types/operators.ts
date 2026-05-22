import i18next from 'i18next';

/**
 * The type of a well-supported operator.
 */
export enum OperatorId {
    /**
     * A value equals another value exactly.
     */
    EQUAL_TO = 'equals',
    /**
     * A value equals another value exactly.
     */
    STARTS_WITH = 'starts_with',
    /**
     * A string contains a specific substring, or a list contains a specific element
     */
    CONTAINS_STR = 'contains_str',
    /**
     * A list contains any items
     */
    CONTAINS_ANY = 'contains_any',
    /**
     * A string matches a specific regex
     */
    REGEX_MATCH = 'regex_match',
    /**
     * The inverse of a nested predicate
     */
    NOT = 'not',
    /**
     * Greater than a value
     */
    GREATER_THAN = 'greater_than',
    /**
     * Less than a value
     */
    LESS_THAN = 'less_than',
    /**
     * Whether an attribute exists at all
     */
    EXISTS = 'exists',
    /**
     * Whether something is true
     */
    IS_TRUE = 'is_true',
    /**
     * Whether something is false
     */
    IS_FALSE = 'is_false',
    /**
     * Specific case for Schema Fields - Schema Fields have a description
     */
    SCHEMA_FIELDS_HAVE_DESCRIPTIONS = 'schema_fields_have_descriptions',
}

/**
 * A single well-supported operator.
 */
export type Operator = {
    id: OperatorId;
    displayName: string;
    description: string;
    operandCount?: number;
};

/**
 * A list of well-supported operators.
 */
const OPERATORS: Operator[] = [
    {
        id: OperatorId.EQUAL_TO,
        get displayName() {
            return i18next.t('shared.query-builder:operators.equals');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.equalsDesc');
        },
    },
    {
        id: OperatorId.CONTAINS_STR,
        get displayName() {
            return i18next.t('shared.query-builder:operators.containsStr');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.containsStrDesc');
        },
    },
    {
        id: OperatorId.CONTAINS_ANY,
        get displayName() {
            return i18next.t('shared.query-builder:operators.containsAny');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.containsAnyDesc');
        },
    },
    {
        id: OperatorId.REGEX_MATCH,
        get displayName() {
            return i18next.t('shared.query-builder:operators.regexMatch');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.regexMatchDesc');
        },
    },
    {
        id: OperatorId.STARTS_WITH,
        get displayName() {
            return i18next.t('shared.query-builder:operators.startsWith');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.startsWithDesc');
        },
    },
    {
        id: OperatorId.NOT,
        get displayName() {
            return i18next.t('shared.query-builder:operators.not');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.notDesc');
        },
    },
    {
        id: OperatorId.GREATER_THAN,
        get displayName() {
            return i18next.t('shared.query-builder:operators.greaterThan');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.greaterThanDesc');
        },
    },
    {
        id: OperatorId.LESS_THAN,
        get displayName() {
            return i18next.t('shared.query-builder:operators.lessThan');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.lessThanDesc');
        },
    },
    {
        id: OperatorId.EXISTS,
        get displayName() {
            return i18next.t('shared.query-builder:operators.exists');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.existsDesc');
        },
        operandCount: 1,
    },
    {
        id: OperatorId.IS_TRUE,
        get displayName() {
            return i18next.t('shared.query-builder:operators.isTrue');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.isTrueDesc');
        },
        operandCount: 1,
    },
    {
        id: OperatorId.IS_FALSE,
        get displayName() {
            return i18next.t('shared.query-builder:operators.isFalse');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.isFalseDesc');
        },
        operandCount: 1,
    },
    {
        id: OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS,
        get displayName() {
            return i18next.t('shared.query-builder:operators.haveDescriptions');
        },
        get description() {
            return i18next.t('shared.query-builder:operators.haveDescriptionsDesc');
        },
        operandCount: 1,
    },
];

/**
 * A map of an operator id to the details used to render the operator.
 */
export const OPERATOR_ID_TO_DETAILS = new Map();

OPERATORS.forEach((operator) => {
    OPERATOR_ID_TO_DETAILS.set(operator.id, operator);
});

/**
 * Returns true if an operator with a particular id
 * is "unary", or takes only one operand.
 *
 * Returns false if the operator cannot be found.
 */
export const isUnaryOperator = (operatorId): boolean => {
    const op = OPERATOR_ID_TO_DETAILS.get(operatorId);
    if (op) {
        return op.operandCount && op.operandCount === 1;
    }
    return false;
};
