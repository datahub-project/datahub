import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { Typography } from 'antd';

export const Header = styled.div`
    && {
        padding-left: 40px;
        padding-right: 40px;
        padding-bottom: 20px;
        padding-top: 20px;
    }
    border-bottom: 1px solid ${colors.gray[100]};
`;

export const Title = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    margin-bottom: 12px;
`;

export const TitleText = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
        color: ${colors.gray[600]};
    }
`;

export const DescriptionText = styled.span`
    font-size: 16px;
    color: ${colors.gray[1700]};
`;

export const Stat = styled.div`
    display: flex;
    align-items: end;
    justify-content: left;
    margin-bottom: 10px;
`;

export const Total = styled.span`
    font-size: 52px;
    line-height: 48px;
    margin-right: 4px;
    color: ${colors.gray[600]};
`;

export const Percent = styled.span`
    font-size: 14px;
    color: ${colors.gray[1700]};
    padding-bottom: 2px;
`;

export const List = styled.div`
    border-color: ${colors.gray[100]};
    border-width: 1px;
`;
