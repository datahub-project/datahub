import React from 'react';
import { Card, Row, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { ArrowRightOutlined, FolderOutlined } from '@ant-design/icons';

export interface BrowseResultProps {
    url: string;
    name: string;
    count?: number | undefined;
    type: string;
}

export default function BrowseResultCard({ url, count, name, type }: BrowseResultProps) {
    return (
        <Link to={url}>
            <Card hoverable>
                <Row style={{ padding: 8 }} justify="space-between">
                    <Space size="middle" align="center">
                        <FolderOutlined width={28} />
                        <Typography.Title style={{ margin: 0 }} level={5}>
                            {name}
                        </Typography.Title>
                    </Space>
                    <Space>
                        {count && (
                            <Typography.Text strong>
                                {count} {type}
                            </Typography.Text>
                        )}
                        <ArrowRightOutlined />
                    </Space>
                </Row>
            </Card>
        </Link>
    );
}
