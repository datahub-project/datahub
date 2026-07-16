import { DeleteOutlined, DownOutlined, PlusOutlined } from '@ant-design/icons';
import { useActive, useCommands } from '@remirror/react';
import { Dropdown, Menu } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('alchemy');
    const active = useActive();
    const commands = useCommands();

    const menu = (
        <Menu>
            <Menu.Item
                icon={<PlusOutlined />}
                disabled={active.tableHeaderCell()}
                onClick={() => commands.addTableRowBefore()}
            >
                {t('editor.table.insertRowAbove')}
            </Menu.Item>
            <Menu.Item icon={<PlusOutlined />} onClick={() => commands.addTableRowAfter()}>
                {t('editor.table.insertRowBelow')}
            </Menu.Item>
            <Menu.Item icon={<PlusOutlined />} onClick={() => commands.addTableColumnBefore()}>
                {t('editor.table.insertColumnLeft')}
            </Menu.Item>
            <Menu.Item icon={<PlusOutlined />} onClick={() => commands.addTableColumnAfter()}>
                {t('editor.table.insertColumnRight')}
            </Menu.Item>
            <Menu.Divider />
            <Menu.Item
                icon={<DeleteOutlined />}
                disabled={active.tableHeaderCell()}
                onClick={() => commands.deleteTableRow()}
            >
                {t('editor.table.deleteRow')}
            </Menu.Item>
            <Menu.Item icon={<DeleteOutlined />} onClick={() => commands.deleteTableColumn()}>
                {t('editor.table.deleteColumn')}
            </Menu.Item>
            <Menu.Item icon={<DeleteOutlined />} onClick={() => commands.deleteTable()}>
                {t('editor.table.deleteTable')}
            </Menu.Item>
        </Menu>
    );

    return (
        <StyledDropdownButton
            size="small"
            icon={<DownOutlined />}
            placement="bottomLeft"
            overlay={menu}
            type="primary"
        />
    );
};
