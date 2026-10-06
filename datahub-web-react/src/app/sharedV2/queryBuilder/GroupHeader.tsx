import { Tooltip } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LogicalOperatorType } from '@app/sharedV2/queryBuilder/builder/types';
import { ActionsContainer, OperationButton, ToolbarContainer } from '@app/sharedV2/queryBuilder/styledComponents';
import { Button as ButtonComponent } from '@src/alchemy-components';

const OperatorButtons = styled.div`
    display: flex;
`;

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
            <OperatorButtons>
                <Tooltip showArrow={false} title={t('group.andTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.AND)}
                        isSelected={selectedOperation === LogicalOperatorType.AND}
                        data-testid="query-builder-all-button"
                    >
                        {t('group.allLabel')}
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title={t('group.orTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.OR)}
                        isSelected={selectedOperation === LogicalOperatorType.OR}
                        data-testid="query-builder-any-button"
                    >
                        {t('group.anyLabel')}
                    </OperationButton>
                </Tooltip>
                <Tooltip showArrow={false} title={t('group.notTooltip')}>
                    <OperationButton
                        variant="text"
                        onClick={(e) => selectOperator(e, LogicalOperatorType.NOT)}
                        isSelected={selectedOperation === LogicalOperatorType.NOT}
                        data-testid="query-builder-none-button"
                    >
                        {t('group.noneLabel')}
                    </OperationButton>
                </Tooltip>
            </OperatorButtons>
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
                {showDeleteButton && (
                    <ButtonComponent
                        variant="text"
                        color="red"
                        icon={{ icon: Trash, size: 'lg', color: 'iconError' }}
                        isCircle
                        onClick={() => onDeletePredicate(index)}
                        data-testid="query-builder-delete-button"
                    />
                )}
            </ActionsContainer>
        </ToolbarContainer>
    );
};

export default GroupHeader;
