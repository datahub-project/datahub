import * as React from 'react';
import { Button } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';
import helpLinkConfig from '../../../conf/HelpLink';

export default function HelpLink() {
    return (
        <a href={helpLinkConfig}>
            <Button type="text">
                <QuestionCircleOutlined />
                FAQ
            </Button>
        </a>
    );
}
