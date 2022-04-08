import * as React from 'react';
import { Button } from 'antd';
import { Link } from 'react-router-dom';
import { EditOutlined } from '@ant-design/icons';

export default function AdhocLink() {
    return (
        <Link to="/adhoc/">
            <Button type="text">
                <EditOutlined />
                Create Dataset
            </Button>
        </Link>
    );
}
