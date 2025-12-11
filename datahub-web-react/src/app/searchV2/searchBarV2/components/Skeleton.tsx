/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
