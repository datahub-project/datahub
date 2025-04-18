import React from 'react';

import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';

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
