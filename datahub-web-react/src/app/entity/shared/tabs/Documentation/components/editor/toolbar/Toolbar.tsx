import {
    BoldOutlined,
    ItalicOutlined,
    OrderedListOutlined,
    StrikethroughOutlined,
    TableOutlined,
    UnderlineOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import { useActive, useCommands } from '@remirror/react';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { AddImageButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/AddImageButton';
import { AddLinkButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/AddLinkButton';
import { CommandButton } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/CommandButton';
import { HeadingMenu } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/HeadingMenu';
import { CodeBlockIcon, CodeIcon } from '@app/entity/shared/tabs/Documentation/components/editor/toolbar/Icons';

const Container = styled.div`
    position: sticky;
    top: 0;
    z-index: 99;
    background-color: #fff;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 4px 20px;
`;

export const Toolbar = () => {
    const commands = useCommands();
    const active = useActive(true);

    return (
        <Container>
            <HeadingMenu />
            <Divider type="vertical" />
            <CommandButton
                icon={<BoldOutlined />}
                commandName="toggleBold"
                active={active.bold()}
                onClick={() => commands.toggleBold()}
            />
            <CommandButton
                icon={<ItalicOutlined />}
                commandName="toggleItalic"
                active={active.italic()}
                onClick={() => commands.toggleItalic()}
            />
            <CommandButton
                icon={<UnderlineOutlined />}
                commandName="toggleUnderline"
                active={active.underline()}
                onClick={() => commands.toggleUnderline()}
            />
            <CommandButton
                icon={<StrikethroughOutlined />}
                commandName="toggleStrike"
                active={active.strike()}
                onClick={() => commands.toggleStrike()}
            />
            <Divider type="vertical" />
            <CommandButton
                icon={<UnorderedListOutlined />}
                commandName="toggleBulletList"
                active={active.bulletList()}
                onClick={() => commands.toggleBulletList()}
            />
            <CommandButton
                icon={<OrderedListOutlined />}
                commandName="toggleOrderedList"
                active={active.orderedList()}
                onClick={() => commands.toggleOrderedList()}
            />
            <Divider type="vertical" />
            <CommandButton
                icon={<CodeIcon />}
                commandName="toggleCode"
                active={active.code()}
                onClick={() => commands.toggleCode()}
            />
            <CommandButton
                icon={<CodeBlockIcon />}
                commandName="toggleCodeBlock"
                active={active.codeBlock()}
                onClick={() => commands.toggleCodeBlock()}
            />
            <Divider type="vertical" />
            <AddImageButton />
            <AddLinkButton />
            <CommandButton
                icon={<TableOutlined />}
                commandName="createTable"
                onClick={() => commands.createTable()}
                disabled={active.table()} /* Disables nested tables */
            />
        </Container>
    );
};
