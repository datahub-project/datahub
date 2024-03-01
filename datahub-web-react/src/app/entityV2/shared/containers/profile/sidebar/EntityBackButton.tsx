import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import { useHistory } from 'react-router';
import KeyboardBackspaceIcon from '@mui/icons-material/KeyboardBackspace';
import { REDESIGN_COLORS } from '../../../constants';

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
        color: #533fd1;
        border-color: #533fd1;
    }
`;

const StyledLeftOutlined = styled(KeyboardBackspaceIcon)`
    && {
        font-size: 20px;
        margin: 0px;
        padding 0px;
    }
`;

export const EntityBackButton = () => {
    const history = useHistory();

    // Temporary hack.
    const hasHistory = (history as any)?.length > 2;

    const onGoBack = () => {
        // TODO: Create a proper navigation history provider.
        // This can result in strange behavior when accessing via direct link.
        // Should do this before we merge.
        // PRD-766
        (history as any).goBack();
    };

    if (!hasHistory) {
        // No button to show.
        return null;
    }

    return (
        <Tooltip title="Go back" showArrow={false} placement="right">
            <StyledButton onClick={onGoBack}>
                <StyledLeftOutlined />
            </StyledButton>
        </Tooltip>
    );
};
