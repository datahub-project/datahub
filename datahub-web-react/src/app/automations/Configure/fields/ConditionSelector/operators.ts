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
export const OPERATORS: Operator[] = [
    {
        id: OperatorId.EQUAL_TO,
        displayName: 'Equals',
        description: 'Exactly equals a value',
    },
    {
        id: OperatorId.CONTAINS_STR,
        displayName: 'Contains',
        description: 'Contains a specific substring',
    },
    {
        id: OperatorId.CONTAINS_ANY,
        displayName: 'Contains Any',
        description: 'Contains any selected items',
    },
    {
        id: OperatorId.REGEX_MATCH,
        displayName: 'Matches Regex',
        description: 'Matches a regex value',
    },
    {
        id: OperatorId.STARTS_WITH,
        displayName: 'Starts with',
        description: 'Starts with a value',
    },
    {
        id: OperatorId.NOT,
        displayName: 'Not',
        description: 'Not',
    },
    {
        id: OperatorId.GREATER_THAN,
        displayName: 'Greater Than',
        description: 'Greater than a value',
    },
    {
        id: OperatorId.LESS_THAN,
        displayName: 'Less Than',
        description: 'Less than a value',
    },
    {
        id: OperatorId.EXISTS,
        displayName: 'Exists',
        description: 'The property exists',
        operandCount: 1,
    },
    {
        id: OperatorId.IS_TRUE,
        displayName: 'Is True',
        description: 'The property value is True',
        operandCount: 1,
    },
    {
        id: OperatorId.IS_FALSE,
        displayName: 'Is False',
        description: 'The property value is False',
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
