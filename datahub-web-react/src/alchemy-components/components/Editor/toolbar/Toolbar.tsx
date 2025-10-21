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
import { useActive, useCommands } from '@remirror/react';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AddImageButton } from '@components/components/Editor/toolbar/AddImageButton';
import { AddLinkButton } from '@components/components/Editor/toolbar/AddLinkButton';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { FontSizeSelect } from '@components/components/Editor/toolbar/FontSizeSelect';
import { HeadingMenu } from '@components/components/Editor/toolbar/HeadingMenu';

import colors from '@src/alchemy-components/theme/foundations/colors';

const Container = styled.div`
    position: sticky;
    top: 0;
    z-index: 99;
    background-color: white;
    border-top-left-radius: 12px;
    border-top-right-radius: 12px;
    padding: 8px !important;
    & button {
        line-height: 0;
    }
    display: flex;
    justify-content: start;
    align-items: center;
    box-shadow: 0 4px 6px -4px rgba(0, 0, 0, 0.1);
    width: 100%;
`;

const InnerContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const CustomDivider = styled(Divider)`
    height: 36px;
    margin: 0;
`;

interface Props {
    styles?: React.CSSProperties;
}

export const Toolbar = ({ styles }: Props) => {
    const commands = useCommands();
    const active = useActive(true);

    return (
        <Container style={styles}>
            <InnerContainer>
                <FontSizeSelect />
                <HeadingMenu />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<TextB size={20} color={colors.gray[1800]} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleBold"
                    active={active.bold()}
                    onClick={() => commands.toggleBold()}
                />
                <CommandButton
                    icon={<TextItalic size={20} color={colors.gray[1800]} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleItalic"
                    active={active.italic()}
                    onClick={() => commands.toggleItalic()}
                />
                <CommandButton
                    icon={<TextUnderline size={20} color={colors.gray[1800]} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleUnderline"
                    active={active.underline()}
                    onClick={() => commands.toggleUnderline()}
                />
                <CommandButton
                    icon={<TextStrikethrough size={20} color={colors.gray[1800]} />}
                    commandName="toggleStrike"
                    active={active.strike()}
                    onClick={() => commands.toggleStrike()}
                />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<ListBullets size={20} color={colors.gray[1800]} />}
                    commandName="toggleBulletList"
                    active={active.bulletList()}
                    onClick={() => commands.toggleBulletList()}
                />
                <CommandButton
                    icon={<ListNumbers size={20} color={colors.gray[1800]} />}
                    commandName="toggleOrderedList"
                    active={active.orderedList()}
                    onClick={() => commands.toggleOrderedList()}
                />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<Code size={20} color={colors.gray[1800]} />}
                    commandName="toggleCode"
                    active={active.code()}
                    onClick={() => commands.toggleCode()}
                />
                <CommandButton
                    icon={<CodeBlock size={20} color={colors.gray[1800]} />}
                    commandName="toggleCodeBlock"
                    active={active.codeBlock()}
                    onClick={() => commands.toggleCodeBlock()}
                />
                <CustomDivider type="vertical" />
                <AddImageButton />
                <AddLinkButton />
                <CommandButton
                    icon={<Table size={20} color={colors.gray[1800]} />}
                    commandName="createTable"
                    onClick={() => commands.createTable()}
                    disabled={active.table()} /* Disables nested tables */
                />
            </InnerContainer>
        </Container>
    );
};
