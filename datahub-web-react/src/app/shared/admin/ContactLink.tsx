import * as React from 'react';
import { Button } from 'antd';
import { MessageOutlined } from '@ant-design/icons';
import contactLinkConfig from '../../../conf/ContactLink';

export default function ContactLink() {
    return (
        <a href={contactLinkConfig}>
            <Button type="text">
                <MessageOutlined />
                ContactUs
            </Button>
        </a>
    );
}
