import { Button, Checkbox, Menu } from 'antd';
import styled from 'styled-components';

export const StyledMenuItem = styled(Menu.Item)`
    min-width: 120px;
    background-color: #fff !important;
    height: 28px;
    font-size: 12px;
    font-weight: 300;
    padding: 12px;
    color: #46507b;
    :hover {
        background-color: transparent;
    }
`;

export const StyledMenu = styled(Menu)`
    border-radius: 4px;
    min-width: 140px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 5px;
    border-right: none;
`;

export const StyledCheckbox = styled(Checkbox)<{ $color?: string }>`
    .ant-checkbox-inner,
    .ant-checkbox-input {
        border-radius: 2px;
        width: 18px !important;
        height: 18px !important;
    }

    .ant-checkbox-checked .ant-checkbox-inner {
        background-color: ${(props) => props.$color};
        border-color: ${(props) => props.$color} !important;
    }

    .ant-checkbox-checked::after {
        border-radius: 2px;
        border-color: ${(props) => props.$color} !important;
    }
`;

export const StyledShareButton = styled(Button)<{ $type?: string; $color?: string; $hoverColor?: string }>`
    && {
        border-radius: 6px;
        height: auto;
        box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.05);
        border-color: ${(props) => props.$color};
        color: ${(props) => (props.$type === 'filled' ? 'white' : props.$color)};
        background-color: ${(props) => (props.$type === 'filled' ? props.$color : 'transparent')};

        :hover {
            border-color: ${(props) => (props.$hoverColor ? props.$hoverColor : props.$color)};
            background-color: ${(props) => (props.$hoverColor ? props.$hoverColor : props.$color)};
            color: ${(props) => !(props.$type === 'filled') && 'white'};
        }
    }
`;
