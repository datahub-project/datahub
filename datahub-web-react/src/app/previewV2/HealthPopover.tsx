import ErrorOutlineOutlinedIcon from '@mui/icons-material/ErrorOutlineOutlined';
import ReportProblemOutlinedIcon from '@mui/icons-material/ReportProblemOutlined';
import VerifiedOutlinedIcon from '@mui/icons-material/VerifiedOutlined';
import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Health, HealthStatus, HealthStatusType } from '../../types.generated';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 180px;

    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 16px;
`;

const Message = styled(Typography.Text)`
    font-size: 12px;
    margin: 5px;
    line-height: 12px;
    font-weight: 400;
    text-align: center;
    display: flex;
`;

const StyledLink = styled(Link)`
    display: flex;
    align-items: center;
    gap: 3px;

    border-radius: 14px;

    :hover {
        background-color: ${REDESIGN_COLORS.BACKGROUND_SECONDARY_GRAY};

        ${Message} {
            text-decoration: underline;
        }
    }
`;

const Icon = styled.div`
    height: 28px;
    width: 28px;
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 50%;
    padding: 5px;
    background: #f7f7f7;
    border: 1px solid #eeeeee;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

interface Props {
    health: Health[];
    baseUrl: string;
}

export default function HealthPopover({ health, baseUrl }: Props) {
    return (
        <Content data-testid="assertions-details">
            {health.map((item) => (
                <StyledLink key={item.type} to={`${baseUrl}${healthUrlSuffix(item)}`}>
                    <Icon>{healthIcon(item)}</Icon>
                    <Message>{healthMessage(item)}</Message>
                </StyledLink>
            ))}
        </Content>
    );
}

function healthIcon({ type }: Health) {
    switch (type) {
        case HealthStatusType.Incidents:
            return <ReportProblemOutlinedIcon fontSize="inherit" />;
        case HealthStatusType.Assertions:
            return <ErrorOutlineOutlinedIcon fontSize="inherit" />;
        case HealthStatusType.Tests:
            return <VerifiedOutlinedIcon fontSize="inherit" />;
        default:
            return null;
    }
}

function healthUrlSuffix({ type }: Health) {
    switch (type) {
        case HealthStatusType.Incidents:
            return '/Incidents';
        case HealthStatusType.Assertions:
            return '/Quality/List';
        case HealthStatusType.Tests:
            return '/Governance/Tests';
        default:
            return null;
    }
}

function healthMessage({ message, status, type }: Health) {
    if (message) return message;
    if (status === HealthStatus.Pass) {
        switch (type) {
            case HealthStatusType.Assertions:
                return 'All assertions are passing';
            case HealthStatusType.Incidents:
                return 'No active incidents';
            case HealthStatusType.Tests:
                return 'No failing governance tests';
            default:
                return null;
        }
    }
    return null;
}
