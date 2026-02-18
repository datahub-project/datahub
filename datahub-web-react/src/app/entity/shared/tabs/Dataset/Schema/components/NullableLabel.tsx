import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

const NullableBadge = styled(Badge)`
    margin-left: 4px;
    &&& .ant-badge-count {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.textBrand};
        border: 1px solid ${(props) => props.theme.colors.borderBrand};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
    }
`;

export default function NullableLabel() {
    return <NullableBadge count="Nullable" />;
}
