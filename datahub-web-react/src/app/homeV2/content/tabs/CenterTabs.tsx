import { Tabs } from '@components';
import { Skeleton } from 'antd';
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import { DEFAULT_TAB } from '@app/homeV2/content/tabs/tabs';
import { useGetActiveTabs } from '@app/homeV2/content/tabs/useGetVisibleTabs';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

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
