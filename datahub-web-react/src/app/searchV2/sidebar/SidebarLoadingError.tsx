import { Alert, Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledAlert = styled(Alert)`
    white-space: normal;
`;

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    return (
        <StyledAlert
            message="The sidebar failed to load."
            showIcon
            type="error"
            action={
                onClickRetry && (
                    <Button size="small" danger onClick={onClickRetry}>
                        Retry
                    </Button>
                )
            }
        />
    );
};

export default SidebarLoadingError;
