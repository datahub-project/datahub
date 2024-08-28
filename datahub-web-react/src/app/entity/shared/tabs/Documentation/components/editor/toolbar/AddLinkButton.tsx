import React, { useState } from 'react';
import { LinkOutlined } from '@ant-design/icons';
import { useActive } from '@remirror/react';

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
                icon={<LinkOutlined />}
                commandName="insertLink"
                onClick={handleButtonClick}
            />
            <LinkModal open={isModalVisible} handleClose={handleClose} />
        </>
    );
};
