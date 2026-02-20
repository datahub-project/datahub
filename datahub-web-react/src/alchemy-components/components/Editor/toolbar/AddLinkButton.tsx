import { LinkSimpleHorizontal } from '@phosphor-icons/react';
import { useActive } from '@remirror/react';
import React, { useState } from 'react';
import { useTheme } from 'styled-components';

import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { LinkModal } from '@components/components/Editor/toolbar/LinkModal';

export const AddLinkButton = () => {
    const [isModalVisible, setModalVisible] = useState(false);
    const styledTheme = useTheme();
    const iconColor = styledTheme.colors.icon;

    const active = useActive(true).link();

    const handleButtonClick = () => {
        setModalVisible(true);
    };

    const handleClose = () => setModalVisible(false);

    return (
        <>
            <CommandButton
                active={active}
                icon={<LinkSimpleHorizontal size={20} color={iconColor} />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
