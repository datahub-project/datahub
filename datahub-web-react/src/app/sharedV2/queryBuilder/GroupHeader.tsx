import { Tooltip } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Button } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('shared.query-builder');
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
                <Tooltip showArrow={false} title={t('group.andTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.AND)}
                        isSelected={selectedOperation === LogicalOperatorType.AND}
                    >
                        {t('group.allLabel')}
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title={t('group.orTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.OR)}
                        isSelected={selectedOperation === LogicalOperatorType.OR}
                    >
                        {t('group.anyLabel')}
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title={t('group.notTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.NOT)}
                        isSelected={selectedOperation === LogicalOperatorType.NOT}
                    >
                        {t('group.noneLabel')}
                    </OperationButton>
                </Tooltip>
            </Button.Group>
            <ActionsContainer>
                <ButtonComponent
                    variant="text"
                    onClick={handleAddPropertyPredicate}
                    data-testid="query-builder-add-condition-button"
                >
                    {t('group.addCondition')}
                </ButtonComponent>
                {!hideAddGroup && (
                    <ButtonComponent
                        variant="text"
                        onClick={handleAddLogicalPredicate}
                        data-testid="query-builder-add-group-button"
                    >
                        {t('group.addGroup')}
                    </ButtonComponent>
                )}
                <CardIcons>
                    {showDeleteButton && (
                        <Icon
                            icon={Trash}
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
