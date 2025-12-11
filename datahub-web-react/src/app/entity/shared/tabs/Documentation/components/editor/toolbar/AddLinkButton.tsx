/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LinkOutlined } from '@ant-design/icons';
import { useActive } from '@remirror/react';
import React, { useState } from 'react';

import { CommandButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/CommandButton';
import { LinkModal } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/LinkModal';

export const AddLinkButton = () => {
    const [isModalVisible, setModalVisible] = useState(false);

    const active = useActive(true).link();

    const handleButtonClick = () => {
        setModalVisible(true);
    };

    const handleClose = () => setModalVisible(false);

    return (
        <>
            <CommandButton
                active={active}
                icon={<LinkOutlined />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal open={isModalVisible} handleClose={handleClose} />
        </>
    );
};
