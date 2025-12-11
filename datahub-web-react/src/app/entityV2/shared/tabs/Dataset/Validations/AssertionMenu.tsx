/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
