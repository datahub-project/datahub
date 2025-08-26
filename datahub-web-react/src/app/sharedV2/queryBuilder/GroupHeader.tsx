import { Tooltip } from '@components';
import { Button } from 'antd';
import React, { useEffect, useState } from 'react';

import { LogicalOperatorType } from '@app/sharedV2/queryBuilder/builder/types';
import {
    ActionsContainer,
    CardIcons,
    OperationButton,
    ToolbarContainer,
} from '@app/sharedV2/queryBuilder/styledComponents';
import { Button as ButtonComponent, Icon } from '@src/alchemy-components';

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
                <ButtonComponent
                    variant="text"
                    onClick={onAddPropertyPredicate}
                    data-testid="query-builder-add-condition-button"
                >
                    Add Condition
                </ButtonComponent>
                <ButtonComponent
                    variant="text"
                    onClick={onAddLogicalPredicate}
                    data-testid="query-builder-add-group-button"
                >
                    Add Group
                </ButtonComponent>
                <CardIcons>
                    <Icon
                        icon="Delete"
                        size="md"
                        onClick={() => onDeletePredicate(index)}
                        data-testid="query-builder-delete-button"
                    />
                </CardIcons>
            </ActionsContainer>
        </ToolbarContainer>
    );
};

export default GroupHeader;
