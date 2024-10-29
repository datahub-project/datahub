import { Button, Checkbox, Form, Menu, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';

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

export const StyledButton = styled(Button)<{ $type?: string; $color?: string; $hoverColor?: string }>`
    && {
        font-size: 14px;
        border-radius: 6px;
        height: auto;
        width: max-content;
        display: flex;
        align-items: center;
        gap: 5px;
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

export const StyledModal = styled(Modal)`
    font-family: Mulish;
    width: 580px !important;
    max-width: 680px;

    &&& .ant-modal-content {
        box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25), 0px 4px 8px 3px rgba(0, 0, 0, 0.15);
        border-radius: 12px;
    }

    .ant-modal-header {
        border-bottom: 0;
        padding-top: 24px;
        border-radius: 12px !important;
    }

    .ant-modal-footer {
        border-top: 0;
    }

    .ant-modal-body {
        padding: 12px 24px;
    }

    .ant-modal-close-x {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        display: flex;
        align-items: center;
        justify-content: center;
        padding-right: 9px;
        padding-top: 20px;

        :hover {
            stroke: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }
`;

export const ModalTitle = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 22px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
`;

export const StyledFormItem = styled(Form.Item)`
    margin-bottom: 8px;

    .ant-input {
        font-size: 14px;
        font-weight: 500;
        border-radius: 8px;
        color: ${REDESIGN_COLORS.FOUNDATION_BLUE_5};

        &:hover,
        &:focus,
        &:active {
            border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }

        &:focus,
        &:active {
            color: ${REDESIGN_COLORS.TITLE_PURPLE};
            box-shadow: 0px 0px 4px 0px rgba(83, 63, 209, 0.5);
        }
    }
`;

export const FormItemTitle = styled(Typography.Text)`
    margin-bottom: 8px;
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;
