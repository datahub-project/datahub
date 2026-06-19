import { Button, Checkbox, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EntityAndType } from '@app/entity/shared/types';
import { SearchSelectActions } from '@app/entityV2/shared/components/styled/search/SearchSelectActions';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityFormContext } from '@src/app/entity/shared/entityForm/EntityFormContext';

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
    }
`;

const SelectionText = styled(Typography.Text)`
    white-space: nowrap;
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
    limit?: number;
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
    limit,
    showCancel = true,
    showActions = true,
    onChangeSelectAll,
    onCancel,
    refetch,
    areAllEntitiesSelected,
    setAreAllEntitiesSelected,
}: Props) => {
    const { t } = useTranslation('entity.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const { isInFormContext } = useEntityFormContext();

    const [showClearSelectionModal, setShowClearSelectionModal] = useState(false);
    const selectedEntityCount = selectedEntities.length;
    const onClickCancel = () => {
        if (selectedEntityCount > 0) {
            setShowClearSelectionModal(true);
        } else {
            onCancel?.();
        }
    };

    return (
        <>
            <CheckboxContainer>
                <StyledCheckbox
                    checked={isSelectAll || areAllEntitiesSelected}
                    onChange={(e) => {
                        onChangeSelectAll(e.target.checked as boolean);
                        setAreAllEntitiesSelected?.(false);
                    }}
                    id="search-select-bar"
                    disabled={limit !== undefined && limit > 0}
                />
                <SelectionText strong type="secondary">
                    {areAllEntitiesSelected
                        ? t('embeddedSearch.allAssetsSelectedCount', { count: totalResults })
                        : t('embeddedSearch.selectedCount', { count: selectedEntityCount })}
                </SelectionText>
                {areAllEntitiesSelected && (
                    <StyledButton
                        type="text"
                        onClick={() => {
                            onChangeSelectAll(false);
                            setAreAllEntitiesSelected?.(false);
                            setSelectedEntities([]);
                        }}
                    >
                        {t('embeddedSearch.clearSelection')}
                    </StyledButton>
                )}
            </CheckboxContainer>
            {!isInFormContext && (
                <ActionsContainer>
                    {showActions && <SearchSelectActions selectedEntities={selectedEntities} refetch={refetch} />}
                    {showCancel && (
                        <CancelButton onClick={onClickCancel} type="link">
                            {tc('done')}
                        </CancelButton>
                    )}
                </ActionsContainer>
            )}
            <ConfirmationModal
                isOpen={showClearSelectionModal}
                handleClose={() => setShowClearSelectionModal(false)}
                handleConfirm={() => onCancel?.()}
                modalTitle={t('embeddedSearch.exitSelectionTitle')}
                modalText={t('embeddedSearch.exitSelectionText', { count: selectedEntityCount })}
            />
        </>
    );
};
