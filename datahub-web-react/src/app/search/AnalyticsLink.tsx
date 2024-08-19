import * as React from 'react';
import { Typography, Tag } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';

const StyledAnalyticsLink = styled(Typography.Text)`
    display: flex;
    margin-right: 20px;
    text-decoration: none;
    && {
        font-size: 14px;
        color: ${(props) => props.theme.styles['layout-header-color']};
    }
    &&:hover {
        color: #000;
    }
`;

export default function AnalyticsLink() {
    const { t } = useTranslation();
    return (
        <Link to="/analytics">
            <StyledAnalyticsLink strong>
                {t('common.analytics')} <Tag>{t('common.beta')}</Tag>
            </StyledAnalyticsLink>
        </Link>
    );
}
