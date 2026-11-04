import { Skeleton as AlchemySkeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const SkeletonContainer = styled.div<{ $width: string }>`
    height: 44px;
    width: ${(props) => props.$width};
`;

export default function Skeleton() {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <SkeletonContainer $width={isShowNavBarRedesign ? '664px' : '620px'}>
            <AlchemySkeleton.Button shape="square" active block height="100%" />
        </SkeletonContainer>
    );
}
