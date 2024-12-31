import React from 'react';
import { Badge } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY_V2 } from '../../../../constants';

type Props = {
    count: number;
};

const ChildCountBadge = styled(Badge)`
    margin-left: 10px;
    margin-top: 16px;
    margin-bottom: 16px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY_V2[1]};
        color: ${ANTD_GRAY_V2[8]};
        box-shadow: 0 2px 1px -1px ${ANTD_GRAY_V2[6]};
        border-radius: 4px 4px 4px 4px;
        font-size: 12px;
        font-weight: 500;
        height: 22px;
        font-family: 'Manrope';
    }
`;

export default function ChildCountLabel({ count }: Props) {
    const propertyString = count > 1 ? ' properties' : ' property';

    // eslint-disable-next-line
    return <ChildCountBadge count={count.toString() + propertyString} />;
}
