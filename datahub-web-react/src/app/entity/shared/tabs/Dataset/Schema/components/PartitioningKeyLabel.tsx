import React from 'react';
import { Badge } from 'antd';
import styled from 'styled-components';
import { blue } from '@ant-design/colors';
import { ANTD_GRAY } from '../../../../constants';

const PartitioningKeyBadge = styled(Badge)`
    margin-left: 4px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${blue[5]};
        border: 1px solid ${blue[2]};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
    }
`;

export default function PartitioningKeyLabel() {
    return <PartitioningKeyBadge count="Partition Key" />;
}
