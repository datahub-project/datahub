/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
    type: 'logical';
    operator: LogicalOperatorType;
    operands: (PropertyPredicate | LogicalPredicate)[];
};

/**
 * Represents a single 'leaf' predicate, which consists of a specific
 * property, an operator, and a set of values.
 */
export interface PropertyPredicate {
    type: 'property';
    property?: string; // -> the identifier of the property. e.g. 'name'
    operator?: string; // -> the identifier of the operator. e.g. 'exists'
    values?: string[]; // -> the value to compare against. e.g. ['something']
}
