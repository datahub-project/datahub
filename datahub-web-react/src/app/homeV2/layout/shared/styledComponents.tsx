/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const ReferenceSectionContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'padding: 0px 12px 0px 12px;'}
    overflow: wrap;
`;

export const ReferenceSectionDivider = styled.hr`
    height: 1px;
    opacity: 0.1;
    width: 100%;
    margin: 20px 0px;
`;

export const ReferenceSection = ({ children }: { children: React.ReactNode }) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    return (
        <ReferenceSectionContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {children}
            <ReferenceSectionDivider />
        </ReferenceSectionContainer>
    );
};
