import React from 'react';
import { Alert, Button } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';

const StyledAlert = styled(Alert)`
    white-space: normal;
`;

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    const { t } = useTranslation();
    return (
        <StyledAlert
            message={t('error.sidebarFailedToLoad')}
            showIcon
            type="error"
            action={
                onClickRetry && (
                    <Button size="small" danger onClick={onClickRetry}>
                        {t('common.retry')}
                    </Button>
                )
            }
        />
    );
};

export default SidebarLoadingError;
