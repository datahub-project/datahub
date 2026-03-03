import { Button, PageTitle, Tabs, colors } from '@components';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import { ViewsList } from '@app/entityV2/view/ViewsList';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';

import { DataHubViewType } from '@types';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    overflow: hidden;
    flex: 1;
    gap: 20px;
    display: flex;
    flex-direction: column;
`;

const PageHeaderContainer = styled.div`
    && {
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
    }
`;

const TitleContainer = styled.div`
    flex: 1;
`;

const HeaderActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const ListContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: ${colors.gray[600]};
    overflow: auto;
`;

enum TabType {
    Personal = 'My Views',
    Global = 'Public Views',
}

const tabUrlMap = {
    [TabType.Personal]: '/settings/views/personal',
    [TabType.Global]: '/settings/views/public',
};

/**
 * Component used for displaying the 'Manage Views' experience.
 */
export const ManageViews = () => {
    const location = useLocation();
    const [showViewBuilder, setShowViewBuilder] = useState(false);
    const [selectedTab, setSelectedTab] = useState<TabType | undefined | null>();

    const onCloseModal = () => {
        setShowViewBuilder(false);
    };

    const tabs: Tab[] = [
        {
            component: <ViewsList viewType={DataHubViewType.Personal} />,
            key: TabType.Personal,
            name: TabType.Personal,
        },
        {
            component: <ViewsList viewType={DataHubViewType.Global} />,
            key: TabType.Global,
            name: TabType.Global,
        },
    ];

    useEffect(() => {
        if (selectedTab === undefined) {
            const currentPath = location.pathname;

            const currentTab = Object.entries(tabUrlMap).find(([, url]) => url === currentPath)?.[0] as TabType;
            if (currentTab) {
                setSelectedTab(currentTab);
            } else {
                setSelectedTab(null);
            }
        }
    }, [selectedTab, location.pathname]);

    return (
        <PageContainer>
            <PageHeaderContainer>
                <TitleContainer>
                    <PageTitle
                        title="Views"
                        subTitle="Create, edit, and remove your Views. Views allow you to save and share sets of filters for reuse when browsing DataHub."
                    />
                </TitleContainer>
                <HeaderActionsContainer>
                    <Button
                        variant="filled"
                        id="create-new-view-button"
                        onClick={() => setShowViewBuilder(true)}
                        data-testid="create-new-view-button"
                        icon={{ icon: 'Plus', source: 'phosphor' }}
                        disabled={false}
                    >
                        Create View
                    </Button>
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <ListContainer>
                <Tabs
                    tabs={tabs}
                    selectedTab={selectedTab as string}
                    onChange={(tab) => setSelectedTab(tab as TabType)}
                    urlMap={tabUrlMap}
                    defaultTab={TabType.Personal}
                />
            </ListContainer>
            {showViewBuilder && (
                <ViewBuilder mode={ViewBuilderMode.EDITOR} onSubmit={onCloseModal} onCancel={onCloseModal} />
            )}
        </PageContainer>
    );
};
