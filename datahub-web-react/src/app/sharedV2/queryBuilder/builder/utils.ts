import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { combineOrFilters } from '@src/app/searchV2/utils/filterUtils';
import { AndFilterInput, FilterOperator } from '@src/types.generated';

const LOGICAL_OPERATORS = new Set(Object.values(LogicalOperatorType));

/**
 * Returns true if the predicate is a logical predicate, as opposed
 * to a property predicate.
 */
export const isLogicalPredicate = (predicate: LogicalPredicate | PropertyPredicate): predicate is LogicalPredicate => {
    const logicalPredicate = predicate as LogicalPredicate;
    return (logicalPredicate.operator && LOGICAL_OPERATORS.has(logicalPredicate.operator)) || false;
};

function mapOperator(operator: string): FilterOperator {
    const operatorMap: { [key: string]: FilterOperator } = {
        // Direct matches from OperatorId enum
        equals: FilterOperator.Equal,
        equal: FilterOperator.Equal,
        startswith: FilterOperator.StartWith,
        containsstr: FilterOperator.Contain,
        contains: FilterOperator.Contain,
        containsany: FilterOperator.In,
        in: FilterOperator.In,
        regexmatch: FilterOperator.Contain, // Regex match treated as contains
        greaterthan: FilterOperator.GreaterThan,
        lessthan: FilterOperator.LessThan,
        exists: FilterOperator.Exists,
        istrue: FilterOperator.Exists,
        isfalse: FilterOperator.Exists, // is_false mapped to Exists with negated: true (handled in caller)
        within: FilterOperator.DescendantsIncl,
        // Legacy/shorthand forms
        descendantsincl: FilterOperator.DescendantsIncl,
        greaterthanorequalto: FilterOperator.GreaterThanOrEqualTo,
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
): AndFilterInput[] | undefined {
    if (pred && 'property' in pred) {
        if (!pred.property) return undefined;

        // it's a PropertyPredicate
        // Special handling for is_false: it means NOT exists, so apply negation
        const operatorIsNegated = isNegated || pred.operator === 'is_false' || pred.operator === 'isfalse';
        return [
            {
                and: [
                    {
                        field: pred.property,
                        values: pred.values || [],
                        condition: pred.operator ? mapOperator(pred.operator) : undefined,
                        ...(operatorIsNegated && { negated: true }),
                    },
                ],
            },
        ];
    }
    if (pred && pred.operator) {
        // it's a LogicalPredicate
        switch (pred.operator) {
            case LogicalOperatorType.AND: {
                const andResults = (pred as LogicalPredicate).operands
                    .map((op) => convertLogicalPredicateToOrFilters(op, isNegated))
                    .filter((filters): filters is AndFilterInput[] => !!filters);
                return andResults.reduce((acc, curr) => combineOrFilters(acc, curr), [{ and: [] }]);
            }
            case LogicalOperatorType.OR:
                return (pred as LogicalPredicate).operands
                    .flatMap((op) => convertLogicalPredicateToOrFilters(op, isNegated))
                    .filter((andFilter): andFilter is AndFilterInput => !!andFilter);
            case LogicalOperatorType.NOT: {
                const notResults = (pred as LogicalPredicate).operands
                    .map((op) => convertLogicalPredicateToOrFilters(op, !isNegated))
                    .filter((filters): filters is AndFilterInput[] => !!filters);
                return notResults.reduce((acc, curr) => combineOrFilters(acc, curr), [{ and: [] }]);
            }
            default:
                console.error(`Unknown operator: ${pred.operator}`);
                return undefined;
        }
    }

    return undefined;
}

export const convertToLogicalPredicate = (predicate: LogicalPredicate | PropertyPredicate): LogicalPredicate => {
    // If we have a property predicate, simply convert to a basic logical predicate.
    if (!isLogicalPredicate(predicate)) {
        return {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [predicate],
        };
    }
    // Already is a logical predicate.
    return predicate as LogicalPredicate;
};

export function isEmptyLogicalPredicate(predicate: LogicalPredicate | null | undefined) {
    return !predicate?.operands?.length;
}
