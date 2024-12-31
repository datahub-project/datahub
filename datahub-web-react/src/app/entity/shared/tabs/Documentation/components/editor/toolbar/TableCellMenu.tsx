import React from 'react';
import { Dropdown } from 'antd';
import styled from 'styled-components';
import { useActive, useCommands } from '@remirror/react';
import { DeleteOutlined, DownOutlined, PlusOutlined } from '@ant-design/icons';
import { MenuItemStyle } from '../../../../../../view/menu/item/styledComponent';

const StyledDropdownButton = styled(Dropdown.Button)`
    position: absolute;
    right: -2px;
    top: 50%;
    transform: translateY(-50%);
    .ant-btn {
        height: auto;
        padding: 0;
        &.ant-btn.ant-btn-icon-only {
            width: 16px;
            height: 16px;
            border-radius: 5px;
        }
    }
`;

export const TableCellMenu = () => {
    const active = useActive();
    const commands = useCommands();

    const divider = {
        key: 'divider',
        type: 'divider',
    };

    const items = [
        {
            key: 0,
            label: (
                <MenuItemStyle
                    icon={<PlusOutlined />}
                    disabled={active.tableHeaderCell()}
                    onClick={() => commands.addTableRowBefore()}
                >
                    Insert row above
                </MenuItemStyle>
            ),
        },
        {
            key: 1,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableRowAfter()}>
                    Insert row below
                </MenuItemStyle>
            ),
        },
        {
            key: 2,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableColumnBefore()}>
                    Insert column left
                </MenuItemStyle>
            ),
        },
        {
            key: 3,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableColumnAfter()}>
                    Insert column right
                </MenuItemStyle>
            ),
        },
        divider,
        {
            key: 4,
            label: (
                <MenuItemStyle
                    icon={<DeleteOutlined />}
                    disabled={active.tableHeaderCell()}
                    onClick={() => commands.deleteTableRow()}
                >
                    Delete row
                </MenuItemStyle>
            ),
        },
        {
            key: 5,
            label: (
                <MenuItemStyle icon={<DeleteOutlined />} onClick={() => commands.deleteTableColumn()}>
                    Delete column
                </MenuItemStyle>
            ),
        },
        {
            key: 6,
            label: (
                <MenuItemStyle icon={<DeleteOutlined />} onClick={() => commands.deleteTable()}>
                    Delete table
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <StyledDropdownButton
            size="small"
            icon={<DownOutlined />}
            placement="bottomLeft"
            menu={{ items }}
            type="primary"
        />
    );
};
