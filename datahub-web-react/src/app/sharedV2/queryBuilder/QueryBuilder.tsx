import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useMemo, useState } from 'react';

import GroupHeader from '@app/sharedV2/queryBuilder/GroupHeader';
/* eslint-disable import/no-cycle */
import Operands from '@app/sharedV2/queryBuilder/Operands';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertToLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';
import { ExpandButton, QueryGroup, QueryGroupHeader } from '@app/sharedV2/queryBuilder/styledComponents';
import { Icon } from '@src/alchemy-components';

const EMPTY_PROPERTY_PREDICATE: PropertyPredicate = {
    type: 'property',
    property: undefined,
    operator: undefined,
    values: undefined,
};

interface Props {
    selectedPredicate: LogicalPredicate | PropertyPredicate;
    onChangePredicate: (newPredicate?: LogicalPredicate) => void;
    properties: Property[];
    depth: number;
    index: number;
    clearFilters?: () => void;
    /** When true, hides the "Add Group" button at every level. */
    hideAddGroup?: boolean;
}

const QueryBuilder = ({
    selectedPredicate,
    onChangePredicate,
    properties,
    depth,
    index,
    clearFilters,
    hideAddGroup,
}: Props) => {
    const [isExpanded, setIsExpanded] = useState(true);

    const logicalPredicate = convertToLogicalPredicate(selectedPredicate);
    const { operator } = logicalPredicate;

    const operands: (PropertyPredicate | LogicalPredicate)[] = useMemo(() => {
        if (logicalPredicate) {
            // Filter out undefined values
            if (logicalPredicate.operands && logicalPredicate.operands.some((item) => item === undefined)) {
                const newOperands = logicalPredicate.operands.filter((item) => item !== undefined);
                onChangePredicate({ type: 'logical', operator, operands: newOperands });
                return newOperands;
            }
            return logicalPredicate.operands;
        }
        return [];
    }, [logicalPredicate, onChangePredicate, operator]);

    const onAddPropertyPredicate = () => {
        const newOperands: (PropertyPredicate | LogicalPredicate)[] = [...operands, EMPTY_PROPERTY_PREDICATE];
        onChangePredicate({ type: 'logical', operator, operands: newOperands });
    };

    const onAddLogicalPredicate = () => {
        const newPredicate: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [],
        };
        const newOperands = [...operands, newPredicate];
        onChangePredicate({ type: 'logical', operator, operands: newOperands });
    };

    const onChangeOperator = (newOperator) => {
        onChangePredicate({ type: 'logical', operator: newOperator, operands });
    };

    const onChangeOperands = (ops) => {
        onChangePredicate({ type: 'logical', operator, operands: ops });
    };

    const onDeletePredicate = () => {
        if (clearFilters) {
            clearFilters();
            return;
        }
        onChangePredicate(undefined);
    };

    const onDeleteCondition = (idx) => {
        const newOperands = [...operands];
        newOperands.splice(idx, 1);
        onChangeOperands(newOperands);
    };

    return (
        <QueryGroup $depth={depth} $hasChildren={operands.length > 0} $isExpanded={isExpanded}>
            <QueryGroupHeader $depth={depth} $hasChildren={operands.length > 0}>
                {operands.length > 0 && (
                    <ExpandButton
                        type="button"
                        onClick={() => setIsExpanded((expanded) => !expanded)}
                        aria-expanded={isExpanded}
                    >
                        <Icon icon={CaretRight} rotate={isExpanded ? '90' : '0'} size="md" color="icon" />
                    </ExpandButton>
                )}
                <GroupHeader
                    onAddLogicalPredicate={onAddLogicalPredicate}
                    onAddPropertyPredicate={onAddPropertyPredicate}
                    onDeletePredicate={onDeletePredicate}
                    onChangeOperator={onChangeOperator}
                    index={index}
                    operator={logicalPredicate.operator}
                    showDeleteButton={operands.length > 0 || depth > 0}
                    hideAddGroup={hideAddGroup}
                />
            </QueryGroupHeader>
            {isExpanded && (
                <Operands
                    operands={operands}
                    onChangeOperands={onChangeOperands}
                    properties={properties}
                    onDeletePredicate={onDeleteCondition}
                    depth={depth}
                    hideAddGroup={hideAddGroup}
                />
            )}
        </QueryGroup>
    );
};

export default QueryBuilder;
