/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider, Row, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

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
