import InfoPopover from '@app/sharedV2/icons/InfoPopover';
import React from 'react';
import { Divider } from 'antd';
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
import styled from 'styled-components';
import { CommandButton } from './CommandButton';
import { HeadingMenu } from './HeadingMenu';
import { AddImageButton } from './AddImageButton';
import { AddLinkButton } from './AddLinkButton';
import { CodeBlockIcon, CodeIcon } from './Icons';
import { REDESIGN_COLORS } from '../../../../../constants';

const Container = styled.div`
    position: sticky;
    top: 0;
    z-index: 99;
    background-color: ${REDESIGN_COLORS.LIGHT_GREY};
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
    border-left: 2px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    padding: 8px 20px !important;
    margin: 2px 14px 2px 12px;
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

const StyledInfoPopover = styled(InfoPopover)`
    font-size: 14px;
`;

const PopoverContent = () => (
    <PopoverContainer>
        <span>
            Reference users and assets using the <strong>@</strong> symbol.
        </span>
    </PopoverContainer>
);

export const Toolbar = () => {
    const commands = useCommands();
    const active = useActive(true);

    return (
        <Container>
            <div>
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
            </div>
            <StyledInfoPopover content={<PopoverContent />} />
        </Container>
    );
};
