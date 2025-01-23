import InfoPopover from '@app/sharedV2/icons/InfoPopover';
import React from 'react';
import { Divider } from 'antd';
import { useActive, useCommands } from '@remirror/react';
import styled from 'styled-components';
import { CommandButton } from './CommandButton';
import { HeadingMenu } from './HeadingMenu';
import { AddImageButton } from './AddImageButton';
import { AddLinkButton } from './AddLinkButton';
import {
    Code,
    CodeBlock,
    ListBullets,
    ListNumbers,
    Table,
    TextB,
    TextItalic,
    TextStrikethrough,
    TextUnderline,
} from '@phosphor-icons/react';
import colors from '@src/alchemy-components/theme/foundations/colors';

const Container = styled.div`
    position: sticky;
    top: 0;
    z-index: 99;
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
    padding: 8px !important;
    & button {
        line-height: 0;
    }
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const PopoverContainer = styled.div`
    display: flex;
    max-width: 450px;
    align-items: baseline;
    strong {
        font-size: 16px;
    }
`;

export const Toolbar = () => {
    const commands = useCommands();
    const active = useActive(true);

    return (
        <Container>
            <HeadingMenu />
            <Divider type="vertical" style={{ height: '100%' }} />
            <CommandButton
                icon={<TextB size={24} color={colors.gray[1800]} />}
                style={{ marginRight: 2 }}
                commandName="toggleBold"
                active={active.bold()}
                onClick={() => commands.toggleBold()}
            />
            <CommandButton
                icon={<TextItalic size={24} color={colors.gray[1800]} />}
                style={{ marginRight: 2 }}
                commandName="toggleItalic"
                active={active.italic()}
                onClick={() => commands.toggleItalic()}
            />
            <CommandButton
                icon={<TextUnderline size={24} color={colors.gray[1800]} />}
                style={{ marginRight: 2 }}
                commandName="toggleUnderline"
                active={active.underline()}
                onClick={() => commands.toggleUnderline()}
            />
            <CommandButton
                icon={<TextStrikethrough size={24} color={colors.gray[1800]} />}
                commandName="toggleStrike"
                active={active.strike()}
                onClick={() => commands.toggleStrike()}
            />
            <Divider type="vertical" style={{ height: '100%' }} />
            <CommandButton
                icon={<ListBullets size={24} color={colors.gray[1800]} />}
                commandName="toggleBulletList"
                active={active.bulletList()}
                onClick={() => commands.toggleBulletList()}
            />
            <CommandButton
                icon={<ListNumbers size={24} color={colors.gray[1800]} />}
                commandName="toggleOrderedList"
                active={active.orderedList()}
                onClick={() => commands.toggleOrderedList()}
            />
            <Divider type="vertical" style={{ height: '100%' }} />
            <CommandButton
                icon={<Code size={24} color={colors.gray[1800]} />}
                commandName="toggleCode"
                active={active.code()}
                onClick={() => commands.toggleCode()}
            />
            <CommandButton
                icon={<CodeBlock size={24} color={colors.gray[1800]} />}
                commandName="toggleCodeBlock"
                active={active.codeBlock()}
                onClick={() => commands.toggleCodeBlock()}
            />
            <Divider type="vertical" style={{ height: '100%' }} />
            <AddImageButton />
            <AddLinkButton />
            <CommandButton
                icon={<Table size={24} />}
                commandName="createTable"
                onClick={() => commands.createTable()}
                disabled={active.table()} /* Disables nested tables */
            />
        </Container>
    );
};
