import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../entity/shared/constants';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    margin-left: 24px;
    margin-bottom: 12px;
    margin-top: 12px;
`;

const Title = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    title: string;
    tooltip?: string;
};

export const TestsSectionTitle = ({ title, tooltip }: Props) => {
    return (
        <Container>
            <Title level={4}>{title}</Title>
            {tooltip && (
                <Tooltip title={tooltip}>
                    <StyledInfoOutlined />
                </Tooltip>
            )}
        </Container>
    );
};
