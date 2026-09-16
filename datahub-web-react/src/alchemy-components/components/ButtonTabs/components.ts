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
`;

export const TabsWrapper = styled.div<{ $fit: TabButtonsFit }>`
    display: flex;
    padding: 2px;
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 6px;
    width: ${(props) => (props.$fit === 'hug' ? 'fit-content' : '100%')};
`;

export const TabContentWrapper = styled.div<{ $visible?: boolean }>`
    ${(props) => !props.$visible && 'display: none;'}
`;
