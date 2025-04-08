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
                icon={<LinkSimpleHorizontal size={24} color={colors.gray[1800]} />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
