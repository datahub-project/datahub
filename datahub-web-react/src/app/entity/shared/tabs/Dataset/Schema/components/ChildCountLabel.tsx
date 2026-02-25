import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

type Props = {
    count: number;
};

const ChildCountBadge = styled(Badge)`
    margin-left: 10px;
    margin-top: 16px;
    margin-bottom: 16px;
    &&& .ant-badge-count {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.textSecondary};
        box-shadow: 0 2px 1px -1px ${(props) => props.theme.colors.textDisabled};
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
