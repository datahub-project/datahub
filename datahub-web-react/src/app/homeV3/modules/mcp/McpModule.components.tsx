import { Icon } from '@components';
import { DotsThree } from '@phosphor-icons/react/dist/csr/DotsThree';
import { OpenAiLogo } from '@phosphor-icons/react/dist/csr/OpenAiLogo';
import React from 'react';
import styled from 'styled-components';

import type { McpModuleClientKey } from '@app/homeV3/modules/mcp/mcpModule.utils';

import claudeLogo from '@images/claudelogo.svg';
import cursorLogo from '@images/cursorlogo.svg';

const LOGO_SIZE = 20;

const LogoImage = styled.img`
    width: ${LOGO_SIZE}px;
    height: ${LOGO_SIZE}px;
    flex-shrink: 0;
    object-fit: contain;
`;

// Cursor's mark is a single near-black shape that would vanish on a dark background, so it is
// drawn as a mask filled with the theme's text color instead of as an image. Vite inlines a small
// SVG as a data URI full of single quotes, so the URL is double-quoted in JS, where Prettier
// can't switch it back to single quotes.
const maskUrl = (src: string) => `url(${JSON.stringify(src)})`;

const MaskedLogo = styled.span<{ $src: string }>`
    width: ${LOGO_SIZE}px;
    height: ${LOGO_SIZE}px;
    flex-shrink: 0;
    background-color: ${(props) => props.theme.colors.text};
    mask: ${(props) => maskUrl(props.$src)} center / contain no-repeat;
    -webkit-mask: ${(props) => maskUrl(props.$src)} center / contain no-repeat;
`;

const Tile = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    min-width: 0;
    height: 40px;
    padding: 0 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 10px;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.text};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        border-color: ${(props) => props.theme.colors.borderHover};
    }
`;

const TileName = styled.span`
    font-size: 14px;
    font-weight: 700;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

/** A client's logo; "Other" gets a neutral icon. Decorative, since the tile names the client. */
export function McpClientLogo({ clientKey }: { clientKey: McpModuleClientKey }) {
    const testId = `mcp-client-logo-${clientKey}`;
    switch (clientKey) {
        case 'claude':
            return <LogoImage src={claudeLogo} alt="" data-testid={testId} />;
        case 'cursor':
            return <MaskedLogo $src={cursorLogo} aria-hidden data-testid={testId} />;
        case 'chatgpt':
            return <Icon icon={OpenAiLogo} size="2xl" aria-hidden data-testid={testId} />;
        default:
            return <Icon icon={DotsThree} size="2xl" aria-hidden data-testid={testId} />;
    }
}

type McpClientTileProps = {
    clientKey: McpModuleClientKey;
    name: string;
    onClick: () => void;
};

/** One client's setup button. */
export function McpClientTile({ clientKey, name, onClick }: McpClientTileProps) {
    return (
        <Tile type="button" onClick={onClick} data-testid={`mcp-client-${clientKey}`}>
            <McpClientLogo clientKey={clientKey} />
            <TileName>{name}</TileName>
        </Tile>
    );
}
