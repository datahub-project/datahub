import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { getHealthRedirectPath, getHealthTypeName } from '@app/shared/health/healthUtils';

import { HealthStatusType } from '@types';

const StatusContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 14px;
`;

const Title = styled.span`
    display: flex;
    align-items: center;
    font-weight: bold;
    margin-right: 8px;
    width: 72px;
`;

const RedirectLink = styled(Link)`
    margin-left: 4px;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

type Props = {
    type: HealthStatusType;
    message?: string | undefined;
    baseUrl: string;
};

export const EntityHealthStatus = ({ type, message, baseUrl }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const title = getHealthTypeName(type);
    const redirectPath = getHealthRedirectPath(type);
    const fullPath = `${baseUrl}/${redirectPath}`;
    return (
        <StatusContainer>
            <Title>{title}</Title> {message}
            {redirectPath && (
                <RedirectLink to={fullPath} data-testid={`${title.toLowerCase()}-details`}>
                    {t('health.detailsLink')}
                </RedirectLink>
            )}
        </StatusContainer>
    );
};
