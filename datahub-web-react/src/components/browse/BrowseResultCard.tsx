import React from 'react';
import { Card } from 'antd';
import { Link } from 'react-router-dom';

export interface BrowseResultProps {
    url: string;
    name: string;
    count?: number | undefined;
}

export default function BrowseResultCard({ url, count, name }: BrowseResultProps) {
    return (
        <Link to={url}>
            <Card hoverable>
                <div style={{ display: 'flex', width: '100%' }}>
                    <div style={{ fontSize: '12px', fontWeight: 'bold', color: '#0073b1' }}>{name}</div>
                    {count && <div style={{ marginLeft: 'auto' }}>{count}</div>}
                </div>
            </Card>
        </Link>
    );
}
