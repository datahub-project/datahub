import { Collapse } from 'antd';
import React, { useMemo, useState } from 'react';

import GroupHeader from '@app/sharedV2/queryBuilder/GroupHeader';
/* eslint-disable import/no-cycle */
import Operands from '@app/sharedV2/queryBuilder/Operands';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { LogicalOperatorType, LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertToLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';
import { CardIcons, StyledCollapse } from '@app/sharedV2/queryBuilder/styledComponents';
import { Icon } from '@src/alchemy-components';

const EMPTY_PROPERTY_PREDICATE = {
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
}

const QueryBuilder = ({ selectedPredicate, onChangePredicate, properties, depth, index, clearFilters }: Props) => {
    const [isExpanded, setIsExpanded] = useState(true);

    const logicalPredicate = convertToLogicalPredicate(selectedPredicate);
    const { operator } = logicalPredicate;

    const operands = useMemo(() => {
        if (logicalPredicate) {
            // Filter out undefined values
            if (logicalPredicate.operands && logicalPredicate.operands.some((item) => item === undefined)) {
                const newOperands = logicalPredicate.operands.filter((item) => item !== undefined);
                onChangePredicate({ operator, operands: newOperands });
                return newOperands;
            }
            return logicalPredicate.operands;
        }
        return [];
    }, [logicalPredicate, onChangePredicate, operator]);

    const onAddPropertyPredicate = () => {
        const newOperands = [...operands, EMPTY_PROPERTY_PREDICATE];
        onChangePredicate({ operator, operands: newOperands });
    };

    const onAddLogicalPredicate = () => {
        const newPredicate = {
            operator: LogicalOperatorType.AND,
            operands: [],
        };
        const newOperands = [...operands, newPredicate];
        onChangePredicate({ operator, operands: newOperands });
    };

    const onChangeOperator = (newOperator) => {
        onChangePredicate({ operator: newOperator, operands });
    };

    const onChangeOperands = (ops) => {
        onChangePredicate({ operator, operands: ops });
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
        <StyledCollapse
            ghost
            expandIcon={({ isActive }) => (
                <CardIcons>
                    <Icon icon="ChevronRight" rotate={isActive ? '90' : '0'} size="md" />
                </CardIcons>
            )}
            defaultActiveKey={`panel-${depth}.${index}`}
            onChange={() => setIsExpanded(!isExpanded)}
            depth={depth}
            hasChildren={operands.length > 0}
            isExpanded={isExpanded}
            collapsible="icon"
        >
            <Collapse.Panel
                key={`panel-${depth}.${index}`}
                header={
                    <GroupHeader
                        onAddLogicalPredicate={onAddLogicalPredicate}
                        onAddPropertyPredicate={onAddPropertyPredicate}
                        onDeletePredicate={onDeletePredicate}
                        onChangeOperator={onChangeOperator}
                        index={index}
                        operator={logicalPredicate.operator}
                    />
                }
                showArrow={operands.length > 0}
            >
                <Operands
                    operands={operands}
                    onChangeOperands={onChangeOperands}
                    properties={properties}
                    onDeletePredicate={onDeleteCondition}
                    depth={depth}
                />
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default QueryBuilder;
