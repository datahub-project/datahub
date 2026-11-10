import { Alert } from '@components';
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
            variant="error"
            title={t('sidebar.loadingError')}
            action={onClickRetry && { label: tc('retry'), onClick: onClickRetry }}
            actionPlacement="topRight"
        />
    );
};

export default SidebarLoadingError;
