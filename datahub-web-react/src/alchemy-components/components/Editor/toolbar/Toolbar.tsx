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
import { useActive, useCommands, useRemirrorContext } from '@remirror/react';
import { Divider } from 'antd';
import React, { useCallback } from 'react';
import styled, { useTheme } from 'styled-components';

import { FileDragDropExtension } from '@components/components/Editor/extensions/fileDragDrop';
import { AddImageButton } from '@components/components/Editor/toolbar/AddImageButton';
import { AddImageButtonV2 } from '@components/components/Editor/toolbar/AddImageButtonV2';
import { AddLinkButton } from '@components/components/Editor/toolbar/AddLinkButton';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { FileUploadButton } from '@components/components/Editor/toolbar/FileUploadButton';
import { FontSizeSelect } from '@components/components/Editor/toolbar/FontSizeSelect';
import { HeadingMenu } from '@components/components/Editor/toolbar/HeadingMenu';

import { useAppConfig } from '@app/useAppConfig';

const Container = styled.div<{ $fixedBottom?: boolean }>`
    position: ${(props) => (props.$fixedBottom ? 'fixed' : 'sticky')};
    ${(props) => (props.$fixedBottom ? 'bottom: 48px;' : 'top: 0;')}
    ${(props) =>
        props.$fixedBottom
            ? 'left: 50%; transform: translateX(-50%); max-width: 800px; width: fit-content;'
            : 'width: 100%;'}
    z-index: ${(props) => (props.$fixedBottom ? '1000' : '99')};
    background-color: ${(props) => props.theme.colors.bg};
    ${(props) =>
        props.$fixedBottom
            ? `border-radius: 12px; border: 1px solid ${props.theme.colors.border};`
            : 'border-top-left-radius: 12px; border-top-right-radius: 12px;'}
    padding: 8px !important;
    & button {
        line-height: 0;
    }
    display: flex;
    justify-content: start;
    align-items: center;
    box-shadow: ${(props) => (props.$fixedBottom ? props.theme.colors.shadowLg : props.theme.colors.shadowSm)};
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
    fixedBottom?: boolean;
}

export const Toolbar = ({ styles, fixedBottom }: Props) => {
    const commands = useCommands();
    const active = useActive(true);
    const theme = useTheme();
    const { config } = useAppConfig();
    const { documentationFileUploadV1 } = config.featureFlags;
    const remirrorContext = useRemirrorContext();
    const fileExtension = remirrorContext.getExtension(FileDragDropExtension);

    const shouldShowImageButtonV2 = documentationFileUploadV1 && fileExtension.options.uploadFileProps?.onFileUpload;

    const handleMouseDown = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
    }, []);

    return (
        <Container style={styles} $fixedBottom={fixedBottom} onMouseDown={handleMouseDown}>
            <InnerContainer>
                <FontSizeSelect />
                <HeadingMenu />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<TextB size={20} color={theme.colors.icon} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleBold"
                    active={active.bold()}
                    onClick={() => commands.toggleBold()}
                />
                <CommandButton
                    icon={<TextItalic size={20} color={theme.colors.icon} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleItalic"
                    active={active.italic()}
                    onClick={() => commands.toggleItalic()}
                />
                <CommandButton
                    icon={<TextUnderline size={20} color={theme.colors.icon} />}
                    style={{ marginRight: 2 }}
                    commandName="toggleUnderline"
                    active={active.underline()}
                    onClick={() => commands.toggleUnderline()}
                />
                <CommandButton
                    icon={<TextStrikethrough size={20} color={theme.colors.icon} />}
                    commandName="toggleStrike"
                    active={active.strike()}
                    onClick={() => commands.toggleStrike()}
                />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<ListBullets size={20} color={theme.colors.icon} />}
                    commandName="toggleBulletList"
                    active={active.bulletList()}
                    onClick={() => commands.toggleBulletList()}
                />
                <CommandButton
                    icon={<ListNumbers size={20} color={theme.colors.icon} />}
                    commandName="toggleOrderedList"
                    active={active.orderedList()}
                    onClick={() => commands.toggleOrderedList()}
                />
                <CustomDivider type="vertical" />
                <CommandButton
                    icon={<Code size={20} color={theme.colors.icon} />}
                    commandName="toggleCode"
                    active={active.code()}
                    onClick={() => commands.toggleCode()}
                />
                <CommandButton
                    icon={<CodeBlock size={20} color={theme.colors.icon} />}
                    commandName="toggleCodeBlock"
                    active={active.codeBlock()}
                    onClick={() => commands.toggleCodeBlock()}
                />
                <CustomDivider type="vertical" />
                {shouldShowImageButtonV2 ? <AddImageButtonV2 /> : <AddImageButton />}
                <AddLinkButton />
                <CommandButton
                    icon={<Table size={20} color={theme.colors.icon} />}
                    commandName="createTable"
                    onClick={() => commands.createTable()}
                    disabled={active.table()} /* Disables nested tables */
                />
                <FileUploadButton />
            </InnerContainer>
        </Container>
    );
};
