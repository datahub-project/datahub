import { InfoCircleOutlined } from '@ant-design/icons';
import { PageTitle, Tooltip, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    margin-bottom: 12px;
    margin-top: 12px;
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${colors.gray[400]};
`;

type Props = {
    title: string;
    tooltip?: string;
};

export const TestsSectionTitle = ({ title, tooltip }: Props) => {
    return (
        <Container>
            <PageTitle title={title} />
            {tooltip && (
                <Tooltip title={tooltip}>
                    <StyledInfoOutlined />
                </Tooltip>
            )}
        </Container>
    );
};
