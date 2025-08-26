import { Tooltip } from '@components';
import { Button, Checkbox, Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { SearchSelectActions } from '@app/entity/shared/components/styled/search/SearchSelectActions';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { EntityAndType } from '@app/entity/shared/types';
import { useAppConfig } from '@src/app/useAppConfig';

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

type Props = {
    isSelectAll: boolean;
    totalResults?: number;
    selectedEntities?: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
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
    setSelectedEntities,
    showCancel = true,
    showActions = true,
    onChangeSelectAll,
    onCancel,
    refetch,
    areAllEntitiesSelected,
    setAreAllEntitiesSelected,
}: Props) => {
    const { isInFormContext } = useEntityFormContext();
    const appConfig = useAppConfig();
    const selectedEntityCount = selectedEntities.length;
    const maxBulkLimit = appConfig.config.testsConfig.executionLimitConfig.elasticSearchExecutor;
    const isBeyondAssetLimit = totalResults >= maxBulkLimit;
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
                {!areAllEntitiesSelected && isInFormContext && isSelectAll && selectedEntityCount < totalResults && (
                    <Tooltip
                        title={
                            isBeyondAssetLimit
                                ? `Don’t worry! You’ll be able to select the rest once the first 10,000 are processed.`
                                : undefined
                        }
                    >
                        <StyledButton type="text" onClick={() => setAreAllEntitiesSelected?.(true)}>
                            {isBeyondAssetLimit
                                ? `Select first ${maxBulkLimit.toLocaleString()} assets`
                                : `Select all ${totalResults} assets`}
                        </StyledButton>
                    </Tooltip>
                )}
                {areAllEntitiesSelected && (
                    <StyledButton
                        type="text"
                        onClick={() => {
                            onChangeSelectAll(false);
                            setAreAllEntitiesSelected?.(false);
                            setSelectedEntities([]);
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
