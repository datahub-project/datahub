import React from 'react';
import { Alert, Button } from 'antd';

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    return (
        <Alert
            message="There was a problem loading the sidebar."
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
