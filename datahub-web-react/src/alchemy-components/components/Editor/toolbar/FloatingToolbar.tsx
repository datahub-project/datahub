import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import {
    BoldOutlined,
    DisconnectOutlined,
    EditOutlined,
    ItalicOutlined,
    LinkOutlined,
    UnderlineOutlined,
} from '@ant-design/icons';
import { FloatingWrapper, useActive, useAttrs, useCommands } from '@remirror/react';
import { createMarkPositioner } from 'remirror/extensions';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';

import { CommandButton } from './CommandButton';
import { LinkModal } from './LinkModal';
import { CodeIcon } from './Icons';

const { Text } = Typography;

export const ToolbarContainer = styled.span`
    display: flex;
    align-items: center;
    padding: 2px;
    background-color: ${ANTD_GRAY[1]};
    border-radius: 4px;
    box-shadow: 0 3px 6px -4px #0000001f, 0 6px 16px #00000014, 0 9px 28px 8px #0000000d;
    overflow: hidden;
    z-index: 300;
`;

const LinkText = styled(Text)`
    padding-left: 4px;
    max-width: 250px;
    text-overflow: ellipsis;
    white-space: nowrap;
    overflow: hidden;
`;

export const FloatingToolbar = () => {
    const [isModalVisible, setModalVisible] = useState(false);
    const commands = useCommands();
    const active = useActive(true);

    const linkPositioner = useMemo(() => createMarkPositioner({ type: 'link' }), []);
    const href = (useAttrs().link()?.href as string) ?? '';

    const handleEditLink = () => {
        setModalVisible(true);
    };

    const handleClose = () => setModalVisible(false);

    const linkCommmands = (
        <ToolbarContainer>
            <LinkText type="secondary">{href}</LinkText>
            <CommandButton size="small" icon={<EditOutlined />} commandName="editLink" onClick={handleEditLink} />
            <CommandButton
                size="small"
                icon={<DisconnectOutlined />}
                commandName="toggleLink"
                onClick={() => commands.removeLink()}
            />
        </ToolbarContainer>
    );

    const shouldShowFloatingToolbar = !(active.link() || active.codeBlock());

    return (
        <>
            <FloatingWrapper positioner={linkPositioner} placement="bottom-start">
                {linkCommmands}
            </FloatingWrapper>
            {shouldShowFloatingToolbar && (
                <FloatingWrapper positioner="selection" placement="top-start">
                    <ToolbarContainer>
                        <CommandButton
                            size="small"
                            icon={<BoldOutlined />}
                            commandName="toggleBold"
                            active={active.bold()}
                            onClick={() => commands.toggleBold()}
                        />
                        <CommandButton
                            size="small"
                            icon={<ItalicOutlined />}
                            commandName="toggleItalic"
                            active={active.italic()}
                            onClick={() => commands.toggleItalic()}
                        />
                        <CommandButton
                            size="small"
                            icon={<UnderlineOutlined />}
                            commandName="toggleUnderline"
                            active={active.underline()}
                            onClick={() => commands.toggleUnderline()}
                        />
                        <CommandButton
                            size="small"
                            icon={<LinkOutlined />}
                            commandName="updateLink"
                            onClick={handleEditLink}
                        />
                        <CommandButton
                            size="small"
                            icon={<CodeIcon />}
                            commandName="toggleCode"
                            active={active.code()}
                            onClick={() => commands.toggleCode()}
                        />
                    </ToolbarContainer>
                </FloatingWrapper>
            )}
            <LinkModal visible={isModalVisible} handleClose={handleClose} />
        </>
    );
};
