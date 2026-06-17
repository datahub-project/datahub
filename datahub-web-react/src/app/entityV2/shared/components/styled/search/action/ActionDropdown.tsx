import { CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Dropdown, Menu } from 'antd';
import MenuItem from 'antd/lib/menu/MenuItem';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const DownArrow = styled(CaretDownOutlined)`
    && {
        padding-top: 4px;
        font-size: 8px;
        margin-left: 2px;
        margin-top: 2px;
        color: ${(props) => props.theme.colors.textTertiary};
    }
`;

const StyledMenuItem = styled(MenuItem)`
    && {
        padding: 0px;
    }
`;

const ActionButton = styled(Button)`
    font-weight: normal;
`;

const DropdownWrapper = styled.div<{
    disabled: boolean;
}>`
    cursor: ${(props) => (props.disabled ? 'normal' : 'pointer')};
    color: ${(props) => (props.disabled ? props.theme.colors.textDisabled : 'none')};
    display: flex;
    margin-left: 12px;
    margin-right: 12px;
`;

type Action = {
    title: React.ReactNode;
    onClick: () => void;
};

type Props = {
    name: string;
    actions: Array<Action>;
    disabled?: boolean;
};

export default function ActionDropdown({ name, actions, disabled }: Props) {
    const { t } = useTranslation('entity.shared.components');
    return (
        <Tooltip title={disabled ? t('searchActions.notSupported') : ''}>
            <Dropdown
                disabled={disabled}
                trigger={['click']}
                overlay={
                    <Menu>
                        {actions.map((action) => (
                            <StyledMenuItem>
                                <ActionButton type="text" onClick={action.onClick}>
                                    {action.title}
                                </ActionButton>
                            </StyledMenuItem>
                        ))}
                    </Menu>
                }
            >
                <DropdownWrapper disabled={!!disabled}>
                    {name}
                    <DownArrow />
                </DropdownWrapper>
            </Dropdown>
        </Tooltip>
    );
}
