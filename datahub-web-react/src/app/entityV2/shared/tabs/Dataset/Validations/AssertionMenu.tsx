import { Menu } from 'antd';
import React from 'react';

import CopyUrnMenuItem from '@app/shared/share/items/CopyUrnMenuItem';

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
