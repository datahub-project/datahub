import { resolveFilterValues } from '@app/entityV2/view/builder/utils';
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
        istrue: FilterOperator.Equal,
        isfalse: FilterOperator.Equal,
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
        return [
            {
                and: [
                    {
                        field: pred.property,
                        values: resolveFilterValues(pred),
                        condition: pred.operator ? mapOperator(pred.operator) : undefined,
                        ...(isNegated && { negated: true }),
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
                const logicalOp = pred as LogicalPredicate;
                // De Morgan's Law: NOT(AND) → OR, NOT(OR) → AND
                if (logicalOp.operands.length === 1 && isLogicalPredicate(logicalOp.operands[0])) {
                    const inner = logicalOp.operands[0] as LogicalPredicate;
                    let swappedOp = inner.operator;
                    if (inner.operator === LogicalOperatorType.AND) {
                        swappedOp = LogicalOperatorType.OR;
                    } else if (inner.operator === LogicalOperatorType.OR) {
                        swappedOp = LogicalOperatorType.AND;
                    }
                    return convertLogicalPredicateToOrFilters(
                        {
                            type: 'logical',
                            operator: swappedOp,
                            operands: inner.operands,
                        },
                        !isNegated,
                    );
                }
                // Fallback: process operands with inverted negation
                const notResults = logicalOp.operands
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
