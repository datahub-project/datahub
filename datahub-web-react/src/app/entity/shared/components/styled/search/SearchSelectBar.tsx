import { Button, Checkbox, Modal, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SearchSelectActions } from '@app/entity/shared/components/styled/search/SearchSelectActions';
import { EntityAndType } from '@app/entity/shared/types';

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
        color: ${(props) => props.theme.colors.textSecondary};
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
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const selectedEntityCount = selectedEntities.length;
    const onClickCancel = () => {
        if (selectedEntityCount > 0) {
            Modal.confirm({
                title: t('searchSelect.exitTitle'),
                content: t('searchSelect.exitContent', { count: selectedEntityCount }),
                onOk() {
                    onCancel?.();
                },
                onCancel() {},
                okText: tc('yes'),
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
                    {t('searchSelect.selectedCount', { count: selectedEntityCount })}
                </Typography.Text>
            </CheckboxContainer>
            <ActionsContainer>
                {showActions && <SearchSelectActions selectedEntities={selectedEntities} refetch={refetch} />}
                {showCancel && (
                    <CancelButton onClick={onClickCancel} type="link">
                        {tc('done')}
                    </CancelButton>
                )}
            </ActionsContainer>
        </>
    );
};
