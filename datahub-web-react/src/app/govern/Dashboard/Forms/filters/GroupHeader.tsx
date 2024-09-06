import { Button as ButtonComponent, Icon } from '@src/alchemy-components';
import { LogicalOperatorType } from '@src/app/tests/builder/steps/definition/builder/types';
import { Button } from 'antd';
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
    const [selectedOperation, setSelectedOperation] = useState<LogicalOperatorType>(operator ?? LogicalOperatorType.OR);

    useEffect(() => {
        onChangeOperator(selectedOperation);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedOperation]);

    return (
        <ToolbarContainer>
            <Button.Group>
                <OperationButton
                    variant="text"
                    onClick={() => setSelectedOperation(LogicalOperatorType.OR)}
                    isSelected={selectedOperation === LogicalOperatorType.OR}
                >
                    Any
                </OperationButton>
                <OperationButton
                    variant="text"
                    onClick={() => setSelectedOperation(LogicalOperatorType.AND)}
                    isSelected={selectedOperation === LogicalOperatorType.AND}
                >
                    All
                </OperationButton>
                <OperationButton
                    variant="text"
                    onClick={() => setSelectedOperation(LogicalOperatorType.NOT)}
                    isSelected={selectedOperation === LogicalOperatorType.NOT}
                >
                    None
                </OperationButton>
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
