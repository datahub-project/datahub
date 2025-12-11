/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider, Space, Typography } from 'antd';
import React from 'react';

import { AvatarsGroup } from '@app/shared/avatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

type Props = {
    definition: string;
    sourceRef: string;
    sourceUrl: string;
    ownership?: any;
};
export default function GlossaryTermHeader({ definition, sourceRef, sourceUrl, ownership }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            <Space direction="vertical" size="middle" style={{ marginBottom: '15px' }}>
                <Typography.Paragraph>{definition}</Typography.Paragraph>
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Source</Typography.Text>
                    <Typography.Text strong>{sourceRef}</Typography.Text>
                    {sourceUrl && (
                        <a href={decodeURIComponent(sourceUrl)} target="_blank" rel="noreferrer">
                            view source
                        </a>
                    )}
                </Space>
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} />
            </Space>
        </>
    );
}
