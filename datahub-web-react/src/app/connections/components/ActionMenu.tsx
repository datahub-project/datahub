import React from 'react';

import styled from 'styled-components';
import { Dropdown, Menu } from 'antd';
import { Tooltip, Icon } from '@components';

import { typography } from '@components/theme';
import { semanticTokens } from '@components/theme/semantic-tokens';

import { ActionMenuItem } from '@src/app/entityV2/shared/EntityDropdown/styledComponents';

const StyledMenuItem = styled(Menu.Item)`
    padding: 4px 12px;
    font-size: ${typography.fontSizes.md};
    font-weight: ${typography.fontWeights.normal};
    color: ${semanticTokens['body-text']};
`;

const StyledTooltip = styled(Tooltip)`
    display: flex;
    align-items: center;
    gap: 6px;
`;

type MenuItem = {
    key: string;
    label: string;
    tooltip?: string;
    icon: string;
    onClick: () => void;
    disabled: boolean;
    hidden?: boolean;
};

interface Props {
    items: MenuItem[];
}

const MenuComponent = ({ items }: Props) => (
    <Menu>
        {items
            .filter((item) => !item.hidden)
            .map((item: any) => (
                <StyledMenuItem key={item.key} onClick={item.onClick} disabled={item.disabled}>
                    <StyledTooltip title={item.tooltip} placement="left">
                        <Icon icon={item.icon} size="md" />
                        {item.label}
                    </StyledTooltip>
                </StyledMenuItem>
            ))}
    </Menu>
);

export const ActionsMenu = ({ items }: Props) => {
    return (
        <ActionMenuItem key="connection-table-view-more">
            <Dropdown dropdownRender={() => <MenuComponent items={items} />} trigger={['click']}>
                <Icon icon="MoreVert" size="lg" />
            </Dropdown>
        </ActionMenuItem>
    );
};
