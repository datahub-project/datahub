import {
    LogicalOperatorType,
    LogicalPredicate,
    PropertyPredicate,
} from '@app/tests/builder/steps/definition/builder/types';
import { AND, NOT, OR } from '@app/tests/builder/steps/definition/constants';
import { AndPredicate, NotPredicate, OrPredicate, Predicate, TestPredicate } from '@app/tests/types';
import { combineOrFilters } from '@src/app/searchV2/utils/filterUtils';
import { AndFilterInput, FilterOperator } from '@src/types.generated';

const LOGICAL_OPERATORS = new Set(Object.values(LogicalOperatorType));

/**
 * Retrieves the display name for a specific Logical Operator Type.
 */
export const getOperatorDisplayName = (operator: LogicalOperatorType) => {
    return operator.toLocaleUpperCase();
};

/**
 * Returns true if the predicate is a logical predicate, as opposed
 * to a property predicate.
 */
export const isLogicalPredicate = (predicate: LogicalPredicate | PropertyPredicate) => {
    const logicalPredicate = predicate as LogicalPredicate;
    return (logicalPredicate.operator && LOGICAL_OPERATORS.has(logicalPredicate.operator)) || false;
};

/**
 * Converts the LogicalPredicateBuilder format into the Metadata Tests predicate format.
 *
 * Metadata Tests leverages a nested logical operator structure:
 *
 * {
 *    and: [
 *      {
 *          property: "entityType",
 *          operator: "equals",
 *          values: ["dataset"]
 *      },
 *      {
 *          or: [
 *              {
 *                  property: "name",
 *                  operator: "exists"
 *              }
 *          ]
 *      }
 *    ]
 * }
 *
 * while the predicate builder component uses a flattened structure:
 *
 * {
 *    operator: "and",
 *    operands: [
 *      {
 *          property: "entityType",
 *          operator: "equals",
 *          values: ["dataset"],
 *      },
 *      {
 *          operator: "or",
 *          operands: [
 *              {
 *                  property: "name",
 *                  operator: "exists"
 *              }
 *          ]
 *      }
 *    ]
 * }
 *
 * @param predicate a union of Logical Predicate (AND, OR, NOT) and Property predicate (a = b)
 */
export const convertLogicalPredicateToTestPredicate = (
    predicate: LogicalPredicate | PropertyPredicate,
): TestPredicate => {
    switch (predicate.operator) {
        case LogicalOperatorType.AND: {
            const andPredicate = predicate as LogicalPredicate;
            return {
                and: andPredicate.operands.map((condition) => convertLogicalPredicateToTestPredicate(condition)),
            };
        }
        case LogicalOperatorType.OR: {
            const orPredicate = predicate as LogicalPredicate;
            return {
                or: orPredicate.operands.map((condition) => convertLogicalPredicateToTestPredicate(condition)),
            };
        }
        case LogicalOperatorType.NOT: {
            const notPredicate = predicate as LogicalPredicate;
            return {
                not: notPredicate.operands.map((condition) => convertLogicalPredicateToTestPredicate(condition)),
            };
        }
        default:
            return predicate as Predicate;
    }
};

/**
 * Recursively converts from the Metadata Tests predicate format into the format required by the
 * Logical Predicate Builder component.
 *
 * This serves as the inverse counterpart to the above function.
 *
 * @param predicate a predicate we've received from a Metadata Test definition
 */
export const convertTestPredicateToLogicalPredicate = (
    predicate: TestPredicate,
): LogicalPredicate | PropertyPredicate => {
    if (Array.isArray(predicate)) {
        const predicates = predicate;
        return {
            operator: LogicalOperatorType.AND,
            operands: predicates.map((pred) => convertTestPredicateToLogicalPredicate(pred)),
        };
    }
    if (AND in predicate) {
        const andPredicate = predicate as AndPredicate;
        return {
            operator: LogicalOperatorType.AND,
            operands: Array.isArray(andPredicate.and)
                ? andPredicate.and.map((pred) => convertTestPredicateToLogicalPredicate(pred))
                : [convertTestPredicateToLogicalPredicate(andPredicate.and)],
        };
    }
    if (OR in predicate) {
        const orPredicate = predicate as OrPredicate;
        return {
            operator: LogicalOperatorType.OR,
            operands: Array.isArray(orPredicate.or)
                ? orPredicate.or.map((pred) => convertTestPredicateToLogicalPredicate(pred))
                : [convertTestPredicateToLogicalPredicate(orPredicate.or)],
        };
    }
    if (NOT in predicate) {
        const notPredicate = predicate as NotPredicate;
        return {
            operator: LogicalOperatorType.NOT,
            operands: Array.isArray(notPredicate.not)
                ? notPredicate.not.map((pred) => convertTestPredicateToLogicalPredicate(pred))
                : [convertTestPredicateToLogicalPredicate(notPredicate.not)],
        };
    }
    // Builder Property Predicate -- These happen to be exactly the same.
    return predicate as PropertyPredicate;
};

function mapOperator(operator: string): FilterOperator {
    const operatorMap: { [key: string]: FilterOperator } = {
        contains: FilterOperator.Contain,
        equals: FilterOperator.Equal,
        equal: FilterOperator.Equal,
        exists: FilterOperator.Exists,
        greaterthan: FilterOperator.GreaterThan,
        greaterthanorequalto: FilterOperator.GreaterThanOrEqualTo,
        in: FilterOperator.In,
        lessthan: FilterOperator.LessThan,
        lessthanorequalto: FilterOperator.LessThanOrEqualTo,
    };

    const normalizedOperator = operator.toLowerCase().replace(/[_\s]/g, '');
    const mappedOperator = operatorMap[normalizedOperator];

    if (!mappedOperator) {
        throw new Error(`Unsupported operator: ${operator}`);
    }

    return mappedOperator;
}

/**
 * Recursively converts a logcal prediate into disjunctive normal form that we expect for our
 * orFilters: AndFilterInput[]
 *
 * @param predicate a LogicalPredicate received from asset selector or some other localtion
 */
export function convertLogicalPredicateToOrFilters(
    pred: LogicalPredicate | PropertyPredicate,
    isNegated = false,
): AndFilterInput[] {
    if (pred && 'property' in pred) {
        // it's a PropertyPredicate
        return [
            {
                and: [
                    {
                        field: pred.property || '',
                        values: pred.values || [],
                        condition: pred.operator ? mapOperator(pred.operator) : undefined,
                        ...(isNegated && { negated: true }),
                    },
                ],
            },
        ];
    }
    // it's a LogicalPredicate
    switch (pred.operator) {
        case LogicalOperatorType.AND: {
            const andResults = (pred as LogicalPredicate).operands.map((op) =>
                convertLogicalPredicateToOrFilters(op, isNegated),
            );
            return andResults.reduce((acc, curr) => combineOrFilters(acc, curr), [{ and: [] }]);
        }
        case LogicalOperatorType.OR:
            return (pred as LogicalPredicate).operands.flatMap((op) =>
                convertLogicalPredicateToOrFilters(op, isNegated),
            );
        case LogicalOperatorType.NOT: {
            const notResults = (pred as LogicalPredicate).operands.map((op) =>
                convertLogicalPredicateToOrFilters(op, !isNegated),
            );
            return notResults.reduce((acc, curr) => combineOrFilters(acc, curr), [{ and: [] }]);
        }
        default:
            throw new Error(`Unknown operator: ${pred.operator}`);
    }
}
