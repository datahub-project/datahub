import { LinkSimpleHorizontal } from '@phosphor-icons/react';
import { useActive } from '@remirror/react';
import React, { useState } from 'react';
import { useTheme } from 'styled-components';

import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { LinkModal } from '@components/components/Editor/toolbar/LinkModal';

import { colors } from '@src/alchemy-components/theme';

export const AddLinkButton = () => {
    const [isModalVisible, setModalVisible] = useState(false);
    const styledTheme = useTheme() as any;
    const iconColor = styledTheme?.colors?.textTertiary ?? colors.gray[1800];

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
