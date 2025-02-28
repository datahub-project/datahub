import React from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { Tooltip } from '@components';
import KeyboardBackspaceIcon from '@mui/icons-material/KeyboardBackspace';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const StyledButton = styled(Button)`
    height: 25px;
    width: 25px;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    padding: 0px;
    border-radius: 20px;
    border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    display: flex;
    align-items: center;
    justify-content: center;
    margin-left: -4px;
    margin-right: 10px;
    margin-top: 2px;

    &:hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const StyledLeftOutlined = styled(KeyboardBackspaceIcon)`
    && {
        font-size: 20px;
        margin: 0px;
        padding 0px;
    }
`;

interface Props {
    onGoBack?: () => void;
}

export const BackButton = ({ onGoBack }: Props) => {
    return (
        <Tooltip title="Go back" showArrow={false} placement="bottom">
            <StyledButton onClick={onGoBack}>
                <StyledLeftOutlined />
            </StyledButton>
        </Tooltip>
    );
};
