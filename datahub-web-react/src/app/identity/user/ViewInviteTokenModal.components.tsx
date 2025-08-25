import { Typography } from 'antd';
import styled from 'styled-components/macro';

export const RecommendationsSection = styled.div`
    margin-top: 20px;
    max-height: 300px;
    overflow-y: auto;
`;

export const UserRecommendationCard = styled.div`
    border: 1px solid ${(props) => props.theme.styles['border-color-base']};
    border-radius: 6px;
    padding: 12px;
    margin-bottom: 8px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background-color: ${(props) => props.theme.styles['background-color']};

    &:hover {
        background-color: ${(props) => props.theme.styles['background-color-light']};
    }
`;

export const UserInfo = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

export const UserName = styled(Typography.Text)`
    font-weight: 600;
    margin-bottom: 2px;
`;

export const UserMetrics = styled.div`
    display: flex;
    gap: 16px;
    margin-top: 4px;
`;

export const MetricBadge = styled.span`
    background-color: ${(props) => props.theme.styles['primary-color-alpha']};
    color: ${(props) => props.theme.styles['primary-color']};
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 500;
`;
