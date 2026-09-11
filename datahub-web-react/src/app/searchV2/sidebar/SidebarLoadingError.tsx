import { Button } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ErrorRow = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
    padding: 8px 12px;
    white-space: normal;
    color: ${(props) => props.theme.colors.textError};
    font-size: 12px;
    line-height: 16px;
`;

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');

    return (
        <ErrorRow>
            <span>{t('sidebar.loadingError')}</span>
            {onClickRetry && (
                <Button variant="text" color="red" size="sm" onClick={onClickRetry}>
                    {tc('retry')}
                </Button>
            )}
        </ErrorRow>
    );
};

export default SidebarLoadingError;
