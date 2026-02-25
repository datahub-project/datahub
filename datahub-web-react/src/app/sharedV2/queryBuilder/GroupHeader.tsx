import { Tooltip } from '@components';
import { Button } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';

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
    showDeleteButton?: boolean;
    /** When true, hides the "Add Group" button (e.g., when the backend model is flat). */
    hideAddGroup?: boolean;
}

const GroupHeader = ({
    onAddPropertyPredicate,
    onAddLogicalPredicate,
    onDeletePredicate,
    onChangeOperator,
    index,
    operator,
    showDeleteButton,
    hideAddGroup,
}: Props) => {
    const [selectedOperation, setSelectedOperation] = useState<LogicalOperatorType>(
        operator ?? LogicalOperatorType.AND,
    );

    useEffect(() => {
        onChangeOperator(selectedOperation);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedOperation]);

    const handleAddPropertyPredicate = useCallback(
        (event: React.MouseEvent) => {
            onAddPropertyPredicate();
            event.preventDefault();
        },
        [onAddPropertyPredicate],
    );

    const handleAddLogicalPredicate = useCallback(
        (event: React.MouseEvent) => {
            onAddLogicalPredicate();
            event.preventDefault();
        },
        [onAddLogicalPredicate],
    );

    const selectOperator = useCallback((event: React.MouseEvent, operatorToSelect: LogicalOperatorType) => {
        setSelectedOperation(operatorToSelect);
        event.preventDefault();
    }, []);

    return (
        <ToolbarContainer>
            <Button.Group>
                <Tooltip showArrow={false} title="Match assets that satisfy all of the following conditions (AND)">
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.AND)}
                        isSelected={selectedOperation === LogicalOperatorType.AND}
                    >
                        All
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title="Match assets that satisfy all of the following conditions (OR)">
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.OR)}
                        isSelected={selectedOperation === LogicalOperatorType.OR}
                    >
                        Any
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title="Match assets that do not match any of the following conditions (NOT)">
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.NOT)}
                        isSelected={selectedOperation === LogicalOperatorType.NOT}
                    >
                        None
                    </OperationButton>
                </Tooltip>
            </Button.Group>
            <ActionsContainer>
                <ButtonComponent
                    variant="text"
                    onClick={handleAddPropertyPredicate}
                    data-testid="query-builder-add-condition-button"
                >
                    Add Condition
                </ButtonComponent>
                {!hideAddGroup && (
                    <ButtonComponent
                        variant="text"
                        onClick={handleAddLogicalPredicate}
                        data-testid="query-builder-add-group-button"
                    >
                        Add Group
                    </ButtonComponent>
                )}
                <CardIcons>
                    {showDeleteButton && (
                        <Icon
                            icon="Delete"
                            size="md"
                            onClick={() => onDeletePredicate(index)}
                            data-testid="query-builder-delete-button"
                        />
                    )}
                </CardIcons>
            </ActionsContainer>
        </ToolbarContainer>
    );
};

export default GroupHeader;
