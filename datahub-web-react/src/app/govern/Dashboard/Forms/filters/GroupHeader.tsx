import { Button as ButtonComponent, Icon } from '@src/alchemy-components';
import { LogicalOperatorType } from '@src/app/tests/builder/steps/definition/builder/types';
import { Button, Tooltip } from 'antd';
import React, { useEffect, useState } from 'react';
import { ActionsContainer, CardIcons, OperationButton, ToolbarContainer } from './styledComponents';

interface Props {
    onAddPropertyPredicate: () => void;
    onAddLogicalPredicate: () => void;
    onDeletePredicate: (index: number) => void;
    onChangeOperator: (operator: LogicalOperatorType) => void;
    index: number;
    operator?: LogicalOperatorType;
}

const GroupHeader = ({
    onAddPropertyPredicate,
    onAddLogicalPredicate,
    onDeletePredicate,
    onChangeOperator,
    index,
    operator,
}: Props) => {
    const [selectedOperation, setSelectedOperation] = useState<LogicalOperatorType>(
        operator ?? LogicalOperatorType.AND,
    );

    useEffect(() => {
        onChangeOperator(selectedOperation);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedOperation]);

    return (
        <ToolbarContainer>
            <Button.Group>
                <Tooltip showArrow={false} title="Match assets that satisfy all of the following conditions (AND)">
                    <OperationButton
                        variant="text"
                        onClick={() => setSelectedOperation(LogicalOperatorType.AND)}
                        isSelected={selectedOperation === LogicalOperatorType.AND}
                    >
                        All
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title="Match assets that satisfy all of the following conditions (OR)">
                    <OperationButton
                        variant="text"
                        onClick={() => setSelectedOperation(LogicalOperatorType.OR)}
                        isSelected={selectedOperation === LogicalOperatorType.OR}
                    >
                        Any
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title="Match assets that do not match any of the following conditions (NOT)">
                    <OperationButton
                        variant="text"
                        onClick={() => setSelectedOperation(LogicalOperatorType.NOT)}
                        isSelected={selectedOperation === LogicalOperatorType.NOT}
                    >
                        None
                    </OperationButton>
                </Tooltip>
            </Button.Group>
            <ActionsContainer>
                <ButtonComponent variant="text" onClick={onAddPropertyPredicate}>
                    Add Condition
                </ButtonComponent>
                <ButtonComponent variant="text" onClick={onAddLogicalPredicate}>
                    Add Group
                </ButtonComponent>
                <CardIcons>
                    <Icon icon="Delete" size="md" onClick={() => onDeletePredicate(index)} />
                </CardIcons>
            </ActionsContainer>
        </ToolbarContainer>
    );
};

export default GroupHeader;
