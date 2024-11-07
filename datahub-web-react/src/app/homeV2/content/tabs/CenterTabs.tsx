import React, { useContext, useState } from 'react';
import styled from 'styled-components';
import { Skeleton } from 'antd';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import OnboardingContext from '../../../onboarding/OnboardingContext';
import { useGetActiveTabs } from './useGetVisibleTabs';
import { DEFAULT_TAB, TAB_NAME_DETAILS, TabType } from './tabs';
import { CenterTab } from './CenterTab';
import { useAppConfig } from '../../../useAppConfig';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    border-radius: 8px;
    padding: 0px 0px 0px 0px;
    ${(props) => !props.$isShowNavBarRedesign && 'margin-top: 18px;'}
`;

const Tabs = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 0px;
    width: auto;
    background-color: #ffffff;
    border-radius: 8px;
    padding: 1px;
`;

const Body = styled.div`
    margin-top: 20px;
    flex: 1;
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
    const [selectedTab, setSelectedTab] = useState<TabType>(activeTabs[0].type || DEFAULT_TAB);
    const selectedTabDetails = TAB_NAME_DETAILS.get(selectedTab);
    const { isUserInitializing } = useContext(OnboardingContext);
    const { loaded } = useAppConfig();

    if (!selectedTabDetails) return null;

    const TabContent = selectedTabDetails.component;

    const updateSelectedTab = (tab: TabType, onSelectTab?: any) => {
        setSelectedTab(tab);
        onSelectTab?.();
    };

    const showSkeleton = isUserInitializing || !loaded;
    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
            {showSkeleton ? (
                <SkeletonButton shape="square" size="large" active />
            ) : (
                <Tabs>
                    {activeTabs.map((tab) => {
                        const details = TAB_NAME_DETAILS.get(tab.type);
                        if (!details) return null;

                        const { name, description, type, icon, id } = details;
                        const { count, onSelectTab } = tab;
                        const selected = selectedTab === type;
                        return (
                            <CenterTab
                                id={id}
                                name={name}
                                description={description}
                                icon={icon}
                                key={type}
                                selected={selected}
                                count={count}
                                onClick={() => updateSelectedTab(type, onSelectTab)}
                            />
                        );
                    })}
                </Tabs>
            )}
            <Body>
                <TabContent />
            </Body>
        </Container>
    );
};
