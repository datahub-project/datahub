import React, { useState } from 'react';
import { useActive } from '@remirror/react';
import { LinkSimpleHorizontal } from '@phosphor-icons/react';
import { colors } from '@src/alchemy-components/theme';

import { CommandButton } from './CommandButton';
import { LinkModal } from './LinkModal';

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
                icon={<LinkSimpleHorizontal size={24} color={colors.gray[1800]} />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
