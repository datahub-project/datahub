import React from 'react';
import { Menu } from 'antd';
import CopyUrnMenuItem from '../../../../../shared/share/items/CopyUrnMenuItem';

interface AssertionMenuProps {
    urn: string;
}

export default function AssertionMenu({ urn }: AssertionMenuProps) {
    return (
        <Menu>
            <CopyUrnMenuItem key="1" urn={urn} type="Assertion" />
        </Menu>
    );
}
