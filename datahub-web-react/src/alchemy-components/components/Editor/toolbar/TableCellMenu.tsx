import { CaretDown, Plus, Trash } from '@phosphor-icons/react';
import { useActive, useCommands } from '@remirror/react';
import { Dropdown, Menu } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledDropdownButton = styled(Dropdown.Button)`
    position: absolute;
    right: 2px;
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

    const menu = (
        <Menu>
            <Menu.Item icon={<Plus />} disabled={active.tableHeaderCell()} onClick={() => commands.addTableRowBefore()}>
                Insert row above
            </Menu.Item>
            <Menu.Item icon={<Plus />} onClick={() => commands.addTableRowAfter()}>
                Insert row below
            </Menu.Item>
            <Menu.Item icon={<Plus />} onClick={() => commands.addTableColumnBefore()}>
                Insert column left
            </Menu.Item>
            <Menu.Item icon={<Plus />} onClick={() => commands.addTableColumnAfter()}>
                Insert column right
            </Menu.Item>
            <Menu.Divider />
            <Menu.Item icon={<Trash />} disabled={active.tableHeaderCell()} onClick={() => commands.deleteTableRow()}>
                Delete row
            </Menu.Item>
            <Menu.Item icon={<Trash />} onClick={() => commands.deleteTableColumn()}>
                Delete column
            </Menu.Item>
            <Menu.Item icon={<Trash />} onClick={() => commands.deleteTable()}>
                Delete table
            </Menu.Item>
        </Menu>
    );

    return (
        <StyledDropdownButton size="small" icon={<CaretDown />} placement="bottomLeft" overlay={menu} type="primary" />
    );
};
