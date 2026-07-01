import { DeleteOutlined, DownOutlined, PlusOutlined } from '@ant-design/icons';
import { useActive, useCommands } from '@remirror/react';
import { Dropdown } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

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
    const { t } = useTranslation('entity.profile.editor');
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
                    {t('table.insertRowAbove')}
                </MenuItemStyle>
            ),
        },
        {
            key: 1,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableRowAfter()}>
                    {t('table.insertRowBelow')}
                </MenuItemStyle>
            ),
        },
        {
            key: 2,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableColumnBefore()}>
                    {t('table.insertColumnLeft')}
                </MenuItemStyle>
            ),
        },
        {
            key: 3,
            label: (
                <MenuItemStyle icon={<PlusOutlined />} onClick={() => commands.addTableColumnAfter()}>
                    {t('table.insertColumnRight')}
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
                    {t('table.deleteRow')}
                </MenuItemStyle>
            ),
        },
        {
            key: 5,
            label: (
                <MenuItemStyle icon={<DeleteOutlined />} onClick={() => commands.deleteTableColumn()}>
                    {t('table.deleteColumn')}
                </MenuItemStyle>
            ),
        },
        {
            key: 6,
            label: (
                <MenuItemStyle icon={<DeleteOutlined />} onClick={() => commands.deleteTable()}>
                    {t('table.deleteTable')}
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
