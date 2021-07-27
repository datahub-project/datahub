import { Divider, Row, Typography } from 'antd';
import styled from 'styled-components';
import React from 'react';

const Section = styled.div`
    padding-top: 24px;
    padding-bottom: 40px;
    margin-bottom: 20px;
    width: 100%;
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

export type Props = {
    children: React.ReactNode;
    title: string;
    rightFloatView?: React.ReactNode;
};

export default function StatsSection({ children, title, rightFloatView }: Props) {
    return (
        <Section>
            <Row justify="space-between">
                <Typography.Title level={3}>{title}</Typography.Title>
                {rightFloatView || <span />}
            </Row>
            <ThinDivider />
            {children}
        </Section>
    );
}
