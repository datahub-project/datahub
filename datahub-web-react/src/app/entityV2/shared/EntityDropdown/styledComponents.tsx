import { Button } from 'antd';
import styled from 'styled-components';

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: ${(props) => props.theme.colors.text};
`;

export const ActionMenuItem = styled(Button)<{ disabled?: boolean; fontSize?: number }>`
    flex-shrink: 0;
    border-radius: 20px;
    width: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    height: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    border: none;
    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.border};
    color: ${(props) => props.theme.colors.icon};
    box-shadow: none;
    &&:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.textHover};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    ${(props) =>
        props.disabled
            ? `
            ${MenuItem} {
                color: ${props.theme.colors.textDisabled};
            }
    `
            : ''};
`;
