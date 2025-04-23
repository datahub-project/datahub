import { Button } from '@components';
import { orderBy, uniq } from 'lodash';
import React, { useMemo, useState } from 'react';

import { AutomationContextProvider } from '@app/automations/Automations/AutomationProvider';
import { AutomationsContextProvider, useAutomationsContext } from '@app/automations/Automations/AutomationsProvider';
import { AutomationCreateModal } from '@app/automations/Automations/CreateModal';
import { EmptyState } from '@app/automations/Automations/EmptyState';
import { AutomationsListCard } from '@app/automations/Automations/ListCard';
import {
    AutomationsBody,
    AutomationsContent,
    AutomationsContentBody,
    AutomationsContentHeader,
    AutomationsContentTab,
    AutomationsContentTabs,
    AutomationsGrid,
    AutomationsPageContainer,
    AutomationsSidebar,
} from '@app/automations/Automations/components';
import { env } from '@app/automations/constants';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const AutomationPage = React.memo(() => {
    // Rollout Variables (UI only)
    const { hideSidebar } = env;

    // Get list from context
    const { automations, isLoading } = useAutomationsContext();

    // Create Modal State
    const [isCreateOpen, setIsCreateOpen] = useState(false);

    // Get Categories
    const categories = useMemo(
        () =>
            uniq(
                automations
                    .map((automation: any) => automation.details?.category)
                    .filter((category) => category !== ''),
            ),
        [automations],
    );

    // Build tabs
    const tabs: any = [
        {
            key: 'all',
            label: 'All',
            data: automations,
            count: automations.length,
        },
    ];
    categories.forEach((category: string) => {
        tabs.push({
            key: category,
            label: category,
            data: automations.filter((automation) => automation.details?.category === category),
            count: automations.filter((automation) => automation.details?.category === category).length,
        });
    });

    const [activeTab, setActiveTab] = useState(tabs[0].key);
    const automationsData = tabs.filter((tab) => tab.key === activeTab)[0].data || [];

    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <>
            <AutomationsPageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                {!hideSidebar && (
                    <AutomationsSidebar>
                        <h1>Sidebar</h1>
                    </AutomationsSidebar>
                )}
                <AutomationsContent>
                    <AutomationsContentHeader $isShowNavBarRedesign={isShowNavBarRedesign}>
                        <PageTitle title="Automations" subTitle="Manage automated actions across your data assets" />
                        <div>
                            <Button size="lg" icon={{ icon: 'Add' }} onClick={() => setIsCreateOpen(!isCreateOpen)}>
                                Create
                            </Button>
                        </div>
                    </AutomationsContentHeader>
                    <AutomationsContentBody $isShowNavBarRedesign={isShowNavBarRedesign}>
                        <AutomationsContentTabs>
                            {orderBy(tabs, ['count'], ['desc']).map((tab) => (
                                <AutomationsContentTab
                                    key={tab.key}
                                    isActive={activeTab === tab.key}
                                    onClick={() => setActiveTab(tab.key)}
                                >
                                    {tab.label}
                                    <span>{tab.count}</span>
                                </AutomationsContentTab>
                            ))}
                        </AutomationsContentTabs>
                        <AutomationsBody>
                            <AutomationsGrid>
                                {automationsData.map((automation) => (
                                    <AutomationsListCard key={automation.key} automation={automation} />
                                ))}
                            </AutomationsGrid>
                            {!isLoading && automationsData && automationsData.length === 0 && <EmptyState />}
                        </AutomationsBody>
                    </AutomationsContentBody>
                </AutomationsContent>
            </AutomationsPageContainer>
            <AutomationContextProvider key="create">
                <AutomationCreateModal isOpen={isCreateOpen} setIsOpen={setIsCreateOpen} />
            </AutomationContextProvider>
        </>
    );
});

// Export the Automations Page with the context provider
export const Automations = () => (
    <AutomationsContextProvider key="create">
        <AutomationPage />
    </AutomationsContextProvider>
);
