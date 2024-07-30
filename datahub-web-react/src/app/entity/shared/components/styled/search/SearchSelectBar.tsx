import React from 'react';
import { Button, Checkbox, Modal, Typography, Tooltip } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { EntityAndType } from '../../../types';
import { SearchSelectActions } from './SearchSelectActions';
import { useEntityFormContext } from '../../../entityForm/EntityFormContext';

const CheckboxContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
`;

const CancelButton = styled(Button)`
    && {
        margin-left: 8px;
        padding: 0px;
        color: ${ANTD_GRAY[7]};
    }
`;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
    padding-bottom: 0px;
`;

const StyledButton = styled(Button)`
    margin-left: 8px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const MAX_BULK_SELECT_COUNT = 10000;

type Props = {
    isSelectAll: boolean;
    totalResults?: number;
    selectedEntities?: EntityAndType[];
    showCancel?: boolean;
    showActions?: boolean;
    onChangeSelectAll: (selected: boolean) => void;
    onCancel?: () => void;
    refetch?: () => void;
    areAllEntitiesSelected?: boolean;
    setAreAllEntitiesSelected?: (areAllSelected: boolean) => void;
};

/**
 * A header for use when an entity search select experience is active.
 *
 * This component provides a select all checkbox and a set of actions that can be taken on the selected entities.
 */
export const SearchSelectBar = ({
    isSelectAll,
    totalResults = 0,
    selectedEntities = [],
    showCancel = true,
    showActions = true,
    onChangeSelectAll,
    onCancel,
    refetch,
    areAllEntitiesSelected,
    setAreAllEntitiesSelected,
}: Props) => {
    const { isInFormContext } = useEntityFormContext();
    const selectedEntityCount = selectedEntities.length;
    const onClickCancel = () => {
        if (selectedEntityCount > 0) {
            Modal.confirm({
                title: `Exit Selection`,
                content: `Are you sure you want to exit? ${selectedEntityCount} selection(s) will be cleared.`,
                onOk() {
                    onCancel?.();
                },
                onCancel() {},
                okText: 'Yes',
                maskClosable: true,
                closable: true,
            });
        } else {
            onCancel?.();
        }
    };

    return (
        <>
            <CheckboxContainer>
                <StyledCheckbox
                    checked={isSelectAll}
                    onChange={(e) => {
                        onChangeSelectAll(e.target.checked as boolean);
                        setAreAllEntitiesSelected?.(false);
                    }}
                />
                <Typography.Text strong type="secondary">
                    {areAllEntitiesSelected ? (
                        <>All {totalResults} assets selected</>
                    ) : (
                        <>{selectedEntityCount} selected</>
                    )}
                </Typography.Text>
                {!areAllEntitiesSelected && isSelectAll && selectedEntityCount < totalResults && (
                    <Tooltip
                        title={
                            totalResults >= MAX_BULK_SELECT_COUNT ? 'Cannot select more than 10,000 assets' : undefined
                        }
                    >
                        <StyledButton
                            type="text"
                            disabled={totalResults >= MAX_BULK_SELECT_COUNT}
                            onClick={() => setAreAllEntitiesSelected?.(true)}
                        >
                            Select all {totalResults} assets
                        </StyledButton>
                    </Tooltip>
                )}
                {areAllEntitiesSelected && (
                    <StyledButton
                        type="text"
                        onClick={() => {
                            onChangeSelectAll(false);
                            setAreAllEntitiesSelected?.(false);
                        }}
                    >
                        Clear selection
                    </StyledButton>
                )}
            </CheckboxContainer>
            {!isInFormContext && (
                <ActionsContainer>
                    {showActions && <SearchSelectActions selectedEntities={selectedEntities} refetch={refetch} />}
                    {showCancel && (
                        <CancelButton onClick={onClickCancel} type="link">
                            Done
                        </CancelButton>
                    )}
                </ActionsContainer>
            )}
        </>
    );
};
