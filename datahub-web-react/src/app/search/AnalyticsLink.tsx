import * as React from 'react';
import { Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const StyledAnalyticsLink = styled(Typography.Text)`
    display: flex;
    margin-right: 20px;
    text-decoration: underline;
    && {
        font-size: 14px;
        color: ${(props) => props.theme.styles['layout-header-color']};
    }
    &&:hover {
        color: #1890ff;
    }
`;

export default function AnalyticsLink() {
    return (
        <Link to="/analytics">
            <StyledAnalyticsLink strong>Analytics [beta]</StyledAnalyticsLink>
        </Link>
    );
}
