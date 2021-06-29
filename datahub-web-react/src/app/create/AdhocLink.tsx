import * as React from 'react';
import { Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const StyledAdhocLink = styled(Typography.Text)`
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

export default function AdhocLink() {
    return (
        <Link to="/adhoc/">
            <StyledAdhocLink strong>Create Dataset</StyledAdhocLink>
        </Link>
    );
}
