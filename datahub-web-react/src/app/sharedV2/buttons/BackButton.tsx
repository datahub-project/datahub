import { Tooltip } from '@components';
import KeyboardBackspaceIcon from '@mui/icons-material/KeyboardBackspace';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { getColor } from '@src/alchemy-components/theme/utils';

const StyledButton = styled(Button)`
    height: 25px;
    width: 25px;
    color: ${(p) => getColor('primary', 500, p.theme)};
    padding: 0px;
    border-radius: 20px;
    border: 1px solid ${(p) => getColor('primary', 500, p.theme)};
    display: flex;
    align-items: center;
    justify-content: center;
    margin-left: -4px;
    margin-right: 10px;
    margin-top: 2px;

    &:hover {
        color: ${(p) => getColor('primary', 500, p.theme)};
        border-color: ${(p) => getColor('primary', 500, p.theme)};
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
