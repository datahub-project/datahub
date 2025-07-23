import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
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

export const convertToLogicalPredicate = (predicate: LogicalPredicate | PropertyPredicate): LogicalPredicate => {
    // If we have a property predicate, simply convert to a basic logical predicate.
    if (!isLogicalPredicate(predicate)) {
        return {
            operator: LogicalOperatorType.AND,
            operands: [predicate],
        };
    }
    // Already is a logical predicate.
    return predicate as LogicalPredicate;
};
