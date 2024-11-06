import React from 'react';
import SkeletonButton from 'antd/lib/skeleton/Button';
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
