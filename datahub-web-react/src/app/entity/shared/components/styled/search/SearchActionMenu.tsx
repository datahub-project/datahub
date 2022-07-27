import React from 'react';
import { Button, Dropdown, Menu } from 'antd';
import { MoreOutlined, PlusOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import type { CheckboxValueType } from 'antd/es/checkbox/Group';

const MenuIcon = styled(MoreOutlined)`
    font-size: 15px;
    height: 20px;
`;

const SelectButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
`;

type Props = {
    checkedSearchResults: CheckboxValueType[];
};

// currently only contains Download As Csv but will be extended to contain other actions as well
export default function SearchActionMenu({ checkedSearchResults }: Props) {
    console.log('checkedSearchResults:: ', checkedSearchResults);
    const menu = (
        <Menu>
            <Menu.Item key="0">
                <SelectButton type="text">
                    <PlusOutlined />
                    Add Tags
                </SelectButton>
            </Menu.Item>
            <Menu.Item key="1">
                <SelectButton type="text">
                    <PlusOutlined />
                    Add Terms
                </SelectButton>
            </Menu.Item>
            <Menu.Item key="2">
                <SelectButton type="text">
                    <PlusOutlined />
                    Add Owners
                </SelectButton>
            </Menu.Item>
        </Menu>
    );

    return (
        <>
            <Dropdown overlay={menu} trigger={['click']}>
                <MenuIcon />
            </Dropdown>
        </>
    );
}
