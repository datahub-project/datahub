import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../constants';
import { ADD_UNAUTHORIZED_MESSAGE } from './utils/constants';

export const PrimaryButton = styled(Button)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    font-size: 14px;
    font-family: Mulish;
    font-weight: 600;
    box-shadow: none;
    border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    margin-left: 9px;
    display: flex;
    align-items: center;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        color: #ffffff;
    }
`;
interface Props {
    buttonLabel?: string;
    isButtonDisabled?: boolean;
    dataTestId?: string;
    onButtonClick?: () => void;
}

const AddButton = ({ buttonLabel, isButtonDisabled, dataTestId, onButtonClick }: Props) => {
    return (
        <Tooltip placement="right" title={(isButtonDisabled && ADD_UNAUTHORIZED_MESSAGE) || 'Add a highlighted query'}>
            <PrimaryButton disabled={isButtonDisabled} onClick={onButtonClick} data-testid={dataTestId}>
                <PlusOutlined /> {buttonLabel}
            </PrimaryButton>
        </Tooltip>
    );
};

export default AddButton;
