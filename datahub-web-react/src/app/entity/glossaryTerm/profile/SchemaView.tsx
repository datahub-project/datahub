/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

export type Props = {
    rawSchema: string | null;
};

const Content = styled.div`
    margin-left: 32px;
    flex-grow: 1;
`;

export default function SchemaView({ rawSchema }: Props) {
    return (
        <>
            {rawSchema && rawSchema.length > 0 ? (
                <Typography.Text data-testid="schema-raw-view">
                    <pre>
                        <code>{rawSchema}</code>
                    </pre>
                </Typography.Text>
            ) : (
                <Content>
                    <Empty description="No Schema" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                </Content>
            )}
        </>
    );
}
