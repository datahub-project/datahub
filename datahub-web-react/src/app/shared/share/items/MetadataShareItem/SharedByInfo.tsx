import { InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';

export default function SharedByInfo() {
    return (
        <Tooltip
            title="This asset was shared along with another asset. This asset could be shared by lineage or due to another relationship to the original shared asset."
            placement="left"
        >
            <InfoCircleOutlined />
        </Tooltip>
    );
}
