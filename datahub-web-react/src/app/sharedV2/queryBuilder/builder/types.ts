/**
 * A type of logical operator - AND, OR, or NOT.
 */
export enum LogicalOperatorType {
    /**
     * And operator
     */
    AND = 'and',
    /**
     * Or operator
     */
    OR = 'or',
    /**
     * Not operator
     */
    NOT = 'not',
}

/**
 * Represents a single compound logical predicate type (AND, OR, or NOT).
 */
export type LogicalPredicate = {
    operator: LogicalOperatorType;
    operands: (PropertyPredicate | LogicalPredicate)[];
};

/**
 * Represents a single 'leaf' predicate, which consists of a specific
 * property, an operator, and a set of values.
 */
export interface PropertyPredicate {
    property?: string; // -> the identifier of the property. e.g. 'name'
    operator?: string; // -> the identifier of the operator. e.g. 'exists'
    values?: string[]; // -> the value to compare against. e.g. ['something']
}
