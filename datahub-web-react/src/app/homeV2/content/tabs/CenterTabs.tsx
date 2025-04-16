import React, { useContext, useState } from 'react';
import styled from 'styled-components';
import { Skeleton } from 'antd';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Tabs } from '@src/alchemy-components';
import OnboardingContext from '../../../onboarding/OnboardingContext';
import { useGetActiveTabs } from './useGetVisibleTabs';
import { DEFAULT_TAB } from './tabs';
import { useAppConfig } from '../../../useAppConfig';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    border-radius: 8px;
    padding: 0px 0px 0px 0px;
    ${(props) => !props.$isShowNavBarRedesign && 'margin-top: 18px;'}
`;

const SkeletonButton = styled(Skeleton.Button)`
    &&& {
        width: 100%;
        height: 44px;
        border-radius: 8px;
    }
`;

export const CenterTabs = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const activeTabs = useGetActiveTabs();
    const [selectedTab, setSelectedTab] = useState<string>(activeTabs[0].key || DEFAULT_TAB);
    const { isUserInitializing } = useContext(OnboardingContext);
    const { loaded } = useAppConfig();

    const showSkeleton = isUserInitializing || !loaded;
    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
            {showSkeleton ? (
                <SkeletonButton shape="square" size="large" active />
            ) : (
                <Tabs selectedTab={selectedTab} onChange={setSelectedTab} tabs={activeTabs} />
            )}
        </Container>
    );
};
