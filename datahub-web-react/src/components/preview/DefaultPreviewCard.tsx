import { Divider, Space } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';

interface Props extends React.PropsWithChildren<any> {
    title?: React.ReactNode;
    url: string;
}

export default function DefaultPreviewCard({ title, url, children }: Props) {
    return (
        <Space direction="vertical" style={{ width: '100%' }}>
            <div style={{ padding: '0% 5%' }}>
                <Link to={url} style={{ color: '#0073b1' }} type="link">
                    {title}
                </Link>
                {children}
            </div>
            <Divider />
        </Space>
    );
}
