import React from 'react';
import styled from 'styled-components';
import ErrorOutlineOutlinedIcon from '@mui/icons-material/ErrorOutlineOutlined';
import ReportProblemOutlinedIcon from '@mui/icons-material/ReportProblemOutlined';
import VerifiedOutlinedIcon from '@mui/icons-material/VerifiedOutlined';
import { Typography } from 'antd';
import { Health, HealthStatusType } from '../../types.generated';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 180px;
`;

const Item = styled.div`
    display: flex;
    align-items: center;
    gap: 3px;
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
    svg {
        font-size: 16px;
        color: #5d668b;
    }
`;
const Message = styled(Typography.Text)`
    font-size: 12px;
    margin: 5px;
    line-height: 12px;
    font-weight: 400;
    color: #374066;
    text-align: center;
    display: flex;
`;

interface Props {
    health: Health[];
}

const HealthPopover = ({ health }: Props) => {
    return (
        <Content>
            {health.map((item) => (
                <Item key={item.type}>
                    {item.message && (
                        <>
                            <Icon>
                                {item.type === HealthStatusType.Assertions && <ErrorOutlineOutlinedIcon />}
                                {item.type === HealthStatusType.Incidents && <ReportProblemOutlinedIcon />}
                                {item.type === HealthStatusType.Tests && <VerifiedOutlinedIcon />}
                            </Icon>
                            <Message>{item.message}</Message>
                        </>
                    )}
                </Item>
            ))}
        </Content>
    );
};

export default HealthPopover;
