import { blue } from '@ant-design/colors';
import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const PrimaryKeyBadge = styled(Badge)`
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

export default function PrimaryKeyLabel() {
    return <PrimaryKeyBadge count="Primary Key" />;
}
