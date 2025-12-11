/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LinkSimpleHorizontal } from '@phosphor-icons/react';
import { useActive } from '@remirror/react';
import React, { useState } from 'react';

import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { LinkModal } from '@components/components/Editor/toolbar/LinkModal';

import { colors } from '@src/alchemy-components/theme';

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
                icon={<LinkSimpleHorizontal size={20} color={colors.gray[1800]} />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
