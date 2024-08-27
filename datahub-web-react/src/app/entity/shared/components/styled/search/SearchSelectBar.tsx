import React from 'react';
import { Button, Checkbox, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { ANTD_GRAY } from '../../../constants';
import { EntityAndType } from '../../../types';
import { SearchSelectActions } from './SearchSelectActions';

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

type Props = {
    isSelectAll: boolean;
    selectedEntities?: EntityAndType[];
    showCancel?: boolean;
    showActions?: boolean;
    onChangeSelectAll: (selected: boolean) => void;
    onCancel?: () => void;
    refetch?: () => void;
};

/**
 * A header for use when an entity search select experience is active.
 *
 * This component provides a select all checkbox and a set of actions that can be taken on the selected entities.
 */
export const SearchSelectBar = ({
    isSelectAll,
    selectedEntities = [],
    showCancel = true,
    showActions = true,
    onChangeSelectAll,
    onCancel,
    refetch,
}: Props) => {
    const { t } = useTranslation();

    const selectedEntityCount = selectedEntities.length;
    const onClickCancel = () => {
        if (selectedEntityCount > 0) {
            Modal.confirm({
                title: t('search.modal.exitSelectionTitle'),
                content: t('search.modal.exitSelectionContent_interval', { count: selectedEntityCount }),
                onOk() {
                    onCancel?.();
                },
                onCancel() {},
                okText: t('common.yes'),
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
                    onChange={(e) => onChangeSelectAll(e.target.checked as boolean)}
                />
                <Typography.Text strong type="secondary">
                    {selectedEntityCount} {t('common.selected')}
                </Typography.Text>
            </CheckboxContainer>
            <ActionsContainer>
                {showActions && <SearchSelectActions selectedEntities={selectedEntities} refetch={refetch} />}
                {showCancel && (
                    <CancelButton onClick={onClickCancel} type="link">
                        {t('common.done')}
                    </CancelButton>
                )}
            </ActionsContainer>
        </>
    );
};
