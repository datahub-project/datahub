import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import styled from 'styled-components';

export const PanelContainer = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 16px;
`;

// Without an explicit type, toggling the panel submits any enclosing form.
export const PanelHeader = styled.button.attrs({ type: 'button' as const })`
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 16px;
    background: ${(props) => props.theme.colors.bgHover};
    border: none;
    cursor: pointer;
    transition: background-color 0.2s;

    &:hover {
        background: ${(props) => props.theme.colors.bgActive};
    }
`;

export const ToggleIcon = styled(CaretDown)<{ $isOpen: boolean }>`
    transition: transform 0.2s;
    transform: ${(props) => (props.$isOpen ? 'rotate(0deg)' : 'rotate(-90deg)')};
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.icon};
`;

export const PanelContent = styled.div<{ $isOpen: boolean }>`
    display: ${(props) => (props.$isOpen ? 'block' : 'none')};
    padding: 16px;
    background: ${(props) => props.theme.colors.bg};
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;
