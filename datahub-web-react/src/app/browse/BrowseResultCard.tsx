import React from 'react';
import { Card, Row, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { ArrowRightOutlined, FolderOutlined } from '@ant-design/icons';
import styled from 'styled-components';

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
}

export default function BrowseResultCard({ url, count, name, type }: BrowseResultProps) {
    return (
        <Link to={url}>
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
                                {count} {type}
                            </Typography.Text>
                        )}
                        <ArrowRightOutlined />
                    </Space>
                </Row>
            </ResultCard>
        </Link>
    );
}
