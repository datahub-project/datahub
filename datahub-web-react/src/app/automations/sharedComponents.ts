import { CheckCircleFilled, CloseCircleFilled } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components';
import { getColor } from '@src/alchemy-components/theme/utils';

import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX } from '@app/entity/shared/tabs/Incident/incidentUtils';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const sharedStyles = {
    gap: '8px',
    borderRadius: '8px',
    boxShadow: '0px 0px 14px 0px rgba(0, 0, 0, 0.10)',

    fontFamily: 'Mulish',
    headingColor: '#374066',
    subHeadingColor: '#323A5D',
    contentColor: '#81879F',
    greyDisabled: '#A3A7B9',

    borderColor: '#EBECF0',
    darkBorderColor: '#D9DBE9',
    dividerColor: '#D9DBE9',

    statusDefaultColor: '#A3A7B9',
    statusDefaultFontColor: '#81879F',

    statusActiveColor: '#deeed5',
    statusActiveFontColor: '#8fc470',

    statusInactiveColor: '#FFE4BD',
    statusInactiveFontColor: '#845B10',

    // Status colors
    success: REDESIGN_COLORS.GREEN_NORMAL,
    fail: REDESIGN_COLORS.RED_NORMAL,
};

/*
 * Icons
 */

export const FailureIcon = styled(CloseCircleFilled)`
    color: ${FAILURE_COLOR_HEX};
    font-size: 18px;
`;

export const SuccessIcon = styled(CheckCircleFilled)`
    color: ${SUCCESS_COLOR_HEX};
    font-size: 20px;
`;

/*
 * Buttons
 */

export const PrimaryButton = styled(Button)`
    display: flex;
    height: 30px;
    padding: 8px 12px;
    justify-content: center;
    align-items: center;
    gap: 4px;

    border-radius: 4px;
    border: 1px solid ${(props) => getColor('primary', 500, props.theme)};
    background-color: ${(props) => getColor('primary', 500, props.theme)};
    box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.05);

    color: #fff;
    font-family: ${sharedStyles.fontFamily};
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;

    &:hover,
    &:focus,
    &:active {
        background-color: #4b39bc;
        border-color: #4b39bc;
        box-shadow: 0px 2px 2px 0px rgba(0, 0, 0, 0.25);
        color: #fff;
    }

    & svg {
        width: 12px;
        height: 12px;
    }
`;

export const LargeButtonPrimary = styled(PrimaryButton)`
    height: 38px;
    padding: 10px 16px;
    border-radius: 6px;
    box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.05);

    font-family: ${sharedStyles.fontFamily};
    font-size: 16px;
    font-weight: 500;
    line-height: normal;

    &:hover,
    &:focus,
    &:active {
        background: #4b39bc;
        box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25);
        color: #f9fafc;
    }
`;

export const SecondaryButton = styled(Button)`
    display: flex;
    height: 30px;
    padding: 8px 12px;
    justify-content: center;
    align-items: center;
    gap: 4px;

    border-radius: 4px;
    border: 1px solid ${(props) => getColor('primary', 500, props.theme)};
    box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.05);

    color: ${(props) => getColor('primary', 500, props.theme)};
    font-family: ${sharedStyles.fontFamily};
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;

    &:hover,
    &:focus,
    &:active {
        border-color: #4b39bc;
        box-shadow: 0px 2px 2px 0px rgba(0, 0, 0, 0.25);
        color: ${(props) => getColor('primary', 500, props.theme)};
    }

    & svg {
        width: 12px;
        height: 12px;
    }
`;

export const TextButton = styled(Button)<{ isActive?: boolean }>`
    display: flex;
    align-items: center;
    width: auto;
    height: auto;
    padding: 0;

    border-radius: 4px;
    border: 0;
    box-shadow: none;
    background-color: transparent !important;

    color: ${(props) => getColor('primary', 500, props.theme)};
    font-family: ${sharedStyles.fontFamily};
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;

    &:hover,
    &:focus,
    &:active {
        border-color: none;
        box-shadow: none;
        color: ${(props) => getColor('primary', 500, props.theme)};
        background-color: transparent;
    }

    & svg {
        width: 12px;
        height: 12px;
        margin-right: 4px;
    }

    ${({ isActive }) =>
        isActive &&
        `
		background: transparent;
		color: ${(props) => getColor('primary', 500, props.theme)};
	`}

    ${({ isActive }) =>
        !isActive &&
        `
		background: transparent;
		color: ${sharedStyles.contentColor};
	`}
`;

export const DeleteButton = styled(Button)`
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid;
    border-color: ${(props) => getColor('primary', 500, props.theme)};
    color: ${(props) => getColor('primary', 500, props.theme)};
    box-shadow: none;

    &:hover,
    &:focus,
    &:active {
        background-color: #f9fafc;
        border-color: ${(props) => getColor('primary', 500, props.theme)};
        color: ${(props) => getColor('primary', 500, props.theme)};
    }

    & svg {
        width: 16px;
        height: 16px;
    }
`;

export const SortButton = styled(Button)`
    display: flex;
    align-times: center;
    justify-content: center;
    border: 0;
    background-color: transparent;
    box-shadow: none;
    padding: 0;
    height: auto;
    width: auto;

    opacity: 0.65;

    &:hover,
    &:focus,
    &:active {
        border: 0;
        background-color: transparent;
        box-shadow: none;
    }
`;

/*
 * Inputs
 */

export const CheckboxGroup = styled.div`
    & .ant-checkbox-group,
    & .ant-radio-group {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        margin-bottom: 8px;
        gap: 8px;
    }

    & .ant-checkbox-wrapper,
    & .ant-radio-wrapper {
        display: flex !important;
        align-items: flex-start;
        flex: 1;
        border: 1px solid ${sharedStyles.borderColor};
        border-radius: 6px;
        padding: 8px;

        & .ant-checkbox,
        & .ant-radio {
            top: 3px;
        }

        &.ant-checkbox-wrapper-checked,
        &.ant-radio-wrapper-checked {
            border-color: ${(props) => getColor('primary', 500, props.theme)};
        }
    }
`;

export const CustomCheckboxLabel = styled.div`
    & strong {
        display: block;
        line-height: 1.4;
        color: ${sharedStyles.subHeadingColor};
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 4px;
        white-space: break-spaces;
    }

    & p {
        color: ${sharedStyles.contentColor};
        font-size: 12px;
        font-weight: 500;
        margin: 0;
    }
`;

/*
 * Typography
 */

export const H3 = styled.h3`
    color: ${sharedStyles.headingColor};
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    margin: 0;
`;

export const P = styled.p`
    color: ${sharedStyles.headingColor};
    font-size: 12px;
    font-weight: 500;
    line-height: 20px;
    margin: 0;
`;
