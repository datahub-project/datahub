import React, { useState } from 'react';
import { useActive } from '@remirror/react';

import { CommandButton } from './CommandButton';
import { LinkModal } from './LinkModal';
import { LinkSimpleHorizontal } from '@phosphor-icons/react';

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
                icon={<LinkSimpleHorizontal size={24} />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
