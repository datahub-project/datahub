import { Button } from '@components';
import styled from 'styled-components';

export const StyledPanelButton = styled(Button).attrs({
    variant: 'text',
})<{ $showText?: boolean }>`
    margin: 2px 0;
    padding: 8px;
    padding-left: ${({ $showText }) => ($showText ? '13px' : '8px')};
    padding-right: ${({ $showText }) => ($showText ? '13px' : '8px')};
    display: flex;
    align-items: center;
    justify-content: ${({ $showText }) => ($showText ? 'flex-start' : 'center')};
    width: 100%;
    color: ${(props) => props.theme.colors.icon};
    gap: 8px;
    white-space: nowrap;

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;
