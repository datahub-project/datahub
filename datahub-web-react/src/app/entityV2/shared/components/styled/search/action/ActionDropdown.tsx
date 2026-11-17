import { Menu, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

const DownArrow = styled(CaretDown).attrs({ size: 10, weight: 'fill' })`
    margin-left: 2px;
    margin-top: 2px;
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const DropdownWrapper = styled.div<{
    disabled: boolean;
}>`
    cursor: ${(props) => (props.disabled ? 'normal' : 'pointer')};
    color: ${(props) => (props.disabled ? props.theme.colors.textDisabled : 'inherit')};
    display: flex;
    align-items: center;
    margin-left: 12px;
    margin-right: 12px;
`;

type Action = {
    title: string;
    onClick: () => void;
};

type Props = {
    name: string;
    actions: Array<Action>;
    disabled?: boolean;
};

export default function ActionDropdown({ name, actions, disabled }: Props) {
    const { t } = useTranslation('entity.shared.components');

    const items: ItemType[] = useMemo(
        () =>
            actions.map((action) => ({
                type: 'item',
                key: action.title,
                title: action.title,
                onClick: action.onClick,
            })),
        [actions],
    );

    return (
        <Tooltip title={disabled ? t('searchActions.notSupported') : ''}>
            <Menu items={items} disabled={disabled} trigger={['click']}>
                <DropdownWrapper disabled={!!disabled}>
                    {name}
                    <DownArrow />
                </DropdownWrapper>
            </Menu>
        </Tooltip>
    );
}
