/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArrowRightOutlined, FolderOutlined } from '@ant-design/icons';
import { Card, Row, Space, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { singularizeCollectionName } from '@app/entity/shared/utils';

const styles = {
    row: { padding: 8 },
    title: { margin: 0 },
};

const ResultCard = styled(Card)`
    && {
        border-color: ${(props) => props.theme.styles['border-color-base']};
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

export interface BrowseResultProps {
    url: string;
    name: string;
    count?: number | undefined;
    type: string;
    onClick?: () => void;
}

export default function BrowseResultCard({ url, count, name, type, onClick }: BrowseResultProps) {
    let displayType = type;
    if (count === 1) {
        displayType = singularizeCollectionName(type);
    }
    return (
        <Link to={url} onClick={onClick}>
            <ResultCard hoverable>
                <Row style={styles.row} justify="space-between">
                    <Space size="middle" align="center">
                        <FolderOutlined width={28} />
                        <Typography.Title style={styles.title} level={5}>
                            {name}
                        </Typography.Title>
                    </Space>
                    <Space>
                        {count && (
                            <Typography.Text strong>
                                {count} {displayType}
                            </Typography.Text>
                        )}
                        <ArrowRightOutlined />
                    </Space>
                </Row>
            </ResultCard>
        </Link>
    );
}
