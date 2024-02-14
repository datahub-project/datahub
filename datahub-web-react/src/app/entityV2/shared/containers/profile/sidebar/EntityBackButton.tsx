import React from 'react';
import styled from 'styled-components';
import { Button, Tooltip } from 'antd';
import { useHistory } from 'react-router';
import KeyboardBackspaceIcon from '@mui/icons-material/KeyboardBackspace';
import { ANTD_GRAY } from '../../../constants';

const StyledButton = styled(Button)`
    height: 28px;
    width: 28px;
    color: ${ANTD_GRAY[7]};
    padding: 0px;
    border-radius: 20px;
    border: 1px solid ${ANTD_GRAY[5]};
    display: flex;
    align-items: center;
    justify-content: center;
    margin-left: -4px;
    margin-right: 10px;
`;

const StyledLeftOutlined = styled(KeyboardBackspaceIcon)`
    && {
        font-size: 18px;
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
