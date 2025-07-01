import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

type Props = {
    close: () => void;
};

export const CloseButton = ({ close }: Props) => {
    return (
        <Button type="text" onClick={close}>
            <ArrowRightOutlined />
        </Button>
    );
};
