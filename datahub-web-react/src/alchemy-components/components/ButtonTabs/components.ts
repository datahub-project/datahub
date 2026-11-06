import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { TabButtonsFit } from '@components/components/ButtonTabs/types';

export const StyledTabButton = styled(Button)<{ $active?: boolean; $fit: TabButtonsFit }>`
    justify-content: center;
    width: ${(props) => (props.$fit === 'fill' ? '100%' : 'auto')};
    flex: ${(props) => (props.$fit === 'fill' ? '1 1 0' : '0 0 auto')};
    min-width: ${(props) => (props.$fit === 'hug' ? '32px' : undefined)};

    ${(props) =>
        props.$active
            ? `
        background: ${props.theme.colors.bg};
        :hover {
            background: ${props.theme.colors.bg};
        }
    `
            : `
        color: ${props.theme.colors.textSecondary} !important;
    `}

    &:disabled {
        color: ${(props) => props.theme.colors.textDisabled} !important;
        cursor: not-allowed;
    }
`;

// Tooltip anchor around a tab: takes the button's place in the flex row. Disabled buttons don't
// emit mouse events, so they let the pointer through to this wrapper instead.
export const TabTooltipAnchor = styled.span<{ $fit: TabButtonsFit }>`
    display: flex;
    flex: ${(props) => (props.$fit === 'fill' ? '1 1 0' : '0 0 auto')};
    min-width: 0;

    button:disabled {
        pointer-events: none;
    }
`;

export const TabsWrapper = styled.div<{ $fit: TabButtonsFit }>`
    display: flex;
    padding: 2px;
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 6px;
    width: ${(props) => (props.$fit === 'hug' ? 'fit-content' : '100%')};
`;

export const ButtonTabsContainer = styled.div<{ $fillHeight?: boolean }>`
    ${(props) =>
        props.$fillHeight &&
        `
        display: flex;
        flex-direction: column;
        flex: 1 1 auto;
        min-height: 0;
    `}
`;

export const TabContentWrapper = styled.div<{ $visible?: boolean; $fillHeight?: boolean }>`
    ${(props) =>
        props.$fillHeight &&
        `
        display: flex;
        flex-direction: column;
        flex: 1 1 auto;
        min-height: 0;
    `}
    ${(props) => !props.$visible && 'display: none;'}
`;
