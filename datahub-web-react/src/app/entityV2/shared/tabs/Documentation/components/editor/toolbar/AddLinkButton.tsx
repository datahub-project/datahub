import { LinkOutlined } from '@ant-design/icons';
import { useActive } from '@remirror/react';
import React, { useState } from 'react';

import { CommandButton } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/CommandButton';
import { LinkModal } from '@app/entityV2/shared/tabs/Documentation/components/editor/toolbar/LinkModal';

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
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
