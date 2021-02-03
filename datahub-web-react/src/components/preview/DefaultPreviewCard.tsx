import { Divider } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';

interface Props extends React.PropsWithChildren<any> {
    title?: React.ReactNode;
    url: string;
}

export default function DefaultPreviewCard({ title, url, children }: Props) {
    return (
        <div style={{ padding: '0px 24px' }}>
            <Link to={url} style={{ color: '#0073b1', padding: '0px 24px' }} type="link">
                {title}
            </Link>
            <div style={{ padding: '20px 5px 5px 5px' }}>{children}</div>
            <Divider />
        </div>
    );
}
