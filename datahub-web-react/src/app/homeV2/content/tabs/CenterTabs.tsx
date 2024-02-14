import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetActiveTabs } from './useGetVisibleTabs';
import { DEFAULT_TAB, TAB_NAME_DETAILS, TabType } from './tabs';
import { CenterTab } from './CenterTab';

const Container = styled.div`
    flex: 1;
    background-color: #f4f5f7;
    border-radius: 8px;
    padding: 0px 0px 0px 0px;
    margin-top: 18px;
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
    margin: 20px 0px 16px 0px;
    flex: 1;
`;

export const CenterTabs = () => {
    const activeTabs = useGetActiveTabs();
    const [selectedTab, setSelectedTab] = useState<TabType>(activeTabs[0].type || DEFAULT_TAB);
    const selectedTabDetails = TAB_NAME_DETAILS.get(selectedTab);
    const TabContent = selectedTabDetails.component;

    const updateSelectedTab = (tab: TabType, onSelectTab?: any) => {
        setSelectedTab(tab);
        onSelectTab?.();
    };

    return (
        <Container>
            <Tabs>
                {activeTabs.map((tab) => {
                    const details = TAB_NAME_DETAILS.get(tab.type);
                    const { name, description, type, icon } = details;
                    const { count, onSelectTab } = tab;
                    const selected = selectedTab === type;
                    return (
                        <CenterTab
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
            <Body>
                <TabContent />
            </Body>
        </Container>
    );
};
