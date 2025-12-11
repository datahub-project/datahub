/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import SkeletonButton from 'antd/lib/skeleton/Button';
import React from 'react';
import styled from 'styled-components';

const Spacer = styled.div`
    flex: 1;
`;

const FlexWrapper = styled.div`
    display: flex;
    gap: 8px;
    width: 100%;
    justify-content: center;
`;

const HeaderWrapper = styled.div`
    margin-top: 10px;
`;

const MainMenuWrapper = styled.div`
    margin-top: 40px;
    display: flex;
    gap: 8px;
    flex-direction: column;
    height: 100%;
`;

type Props = {
    isCollapsed?: boolean;
};

export default function NavSkeleton({ isCollapsed }: Props) {
    const menuItem = (
        <FlexWrapper>
            <SkeletonButton
                active
                style={{ width: isCollapsed ? '24px' : '235px', height: '24px', minWidth: '24px' }}
            />
        </FlexWrapper>
    );

    return (
        <>
            <HeaderWrapper>{menuItem}</HeaderWrapper>
            <MainMenuWrapper>
                {menuItem}
                {menuItem}
                {menuItem}
                {menuItem}
                {menuItem}
                {menuItem}
                <Spacer />
                {menuItem}
                {menuItem}
                {menuItem}
            </MainMenuWrapper>
        </>
    );
}
