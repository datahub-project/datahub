import { Alert, Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledAlert = styled(Alert)`
    white-space: normal;
`;

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');

    return (
        <StyledAlert
            message={t('sidebar.loadingError')}
            showIcon
            type="error"
            action={
                onClickRetry && (
                    <Button size="small" danger onClick={onClickRetry}>
                        {tc('retry')}
                    </Button>
                )
            }
        />
    );
};

export default SidebarLoadingError;
