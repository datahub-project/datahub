import { Skeleton as AntdSkeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const SkeletonContainer = styled.div<{ $width: string }>`
    height: 44px;
    width: ${(props) => props.$width};
`;

const SkeletonButton = styled(AntdSkeleton.Button)`
    &&& {
        height: inherit;
        width: inherit;
    }
`;

export default function Skeleton() {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <SkeletonContainer $width={isShowNavBarRedesign ? '664px' : '620px'}>
            <SkeletonButton shape="square" active block />
        </SkeletonContainer>
    );
}
