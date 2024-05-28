import React, { useState } from 'react';
import styled from 'styled-components';

import { uniq } from 'lodash';

import { notification } from 'antd';
import type { NotificationPlacement } from 'antd/es/notification';
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { useListTestsQuery } from '../../../graphql/test.generated';
import { useListActionPipelinesQuery } from '../../../graphql/actionPipeline.generated';

import {
    AutomationsPageContainer,
    AutomationsSidebar,
    AutomationsContent,
    AutomationsContentHeader,
    AutomationsContentBody,
    AutomationsContentTabs,
    AutomationsContentTab,
    AutomationsBody,
} from './components';

import { LargeButtonPrimary } from '../sharedComponents';

import { simplifyDataForListView } from '../utils';

import { AutomationsListCard } from './ListCard';
import { AutomationCreateModal } from './CreateModal';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const NotificationTitleContainer = styled.div`
    font-weight: 700;
    line-height: 18px;
    display: flex;
    flex-direction: column;
    gap: 3px;
    margin: 0px 0px 1em -32px;
    font-family: Mulish;
`;

const NotificationHeader = styled.div`
    font-size: 12px;
`;

const NotificationTitle = styled.div`
    font-size: 21px;
`;

const NotificationDescriptionContainer = styled.div`
    font-family: Mulish;
    font-size: 14px;
    font-weight: 400;
    line-height: 18px;
    letter-spacing: 0.25px;
    margin-left: -32px;
    display: flex;
    margin-right: 25px;
    justify-content: space-between;
`;

const ViewLogsContainer = styled.div`
    cursor: pointer;
`;

export const Automations = () => {
    // Create Modal State
    const [isOpen, setIsOpen] = useState(false);

    const notificationConfig = {
        duration: 3,
        icon: <></>,
        closeIcon: <CloseOutlinedIcon />,
    };

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const openErrorNotification = (placement: NotificationPlacement) => {
        notification.error({
            message: (
                <NotificationTitleContainer>
                    <NotificationHeader>PROPAGATION</NotificationHeader>
                    <NotificationTitle>Automation Title</NotificationTitle>
                </NotificationTitleContainer>
            ),
            description: (
                <NotificationDescriptionContainer>
                    <div>Failed to apply undo operation</div>
                    <ViewLogsContainer>View Logs</ViewLogsContainer>
                </NotificationDescriptionContainer>
            ),
            placement,
            style: {
                backgroundColor: `${REDESIGN_COLORS.RED_LIGHT}`,
                borderLeft: `4px solid ${REDESIGN_COLORS.DEPRECATION_RED_LIGHT}`,
                padding: '15px 0px',
            },
            ...notificationConfig,
        });
    };

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const openSuccessNotification = (placement: NotificationPlacement) => {
        notification.success({
            message: (
                <NotificationTitleContainer>
                    <NotificationHeader>PROPAGATION</NotificationHeader>
                    <NotificationTitle>Automation Title</NotificationTitle>
                </NotificationTitleContainer>
            ),
            description: (
                <NotificationDescriptionContainer>
                    <div>Successful Automation of 135 Assets</div>
                    <ViewLogsContainer>View Logs</ViewLogsContainer>
                </NotificationDescriptionContainer>
            ),
            placement,
            style: {
                backgroundColor: `${REDESIGN_COLORS.GREEN_LIGHT}`,
                borderLeft: `4px solid ${REDESIGN_COLORS.GREEN_NORMAL}`,
                padding: '15px 0px',
            },
            ...notificationConfig,
        });
    };

    // Fetch metadata tests
    const { data: metadataTestsData } = useListTestsQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    // Fetch action pipelines
    const { data: actionPipelinesData } = useListActionPipelinesQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    // Raw Data
    const metadataTests = metadataTestsData?.listTests?.tests || [];
    const actionPipelines = actionPipelinesData?.listActionPipelines?.actionPipelines || [];

    // Simplify Data for List View
    const simplifiedMetadataTests = simplifyDataForListView(metadataTests);
    const simplifiedActionPipelines = simplifyDataForListView(actionPipelines);

    // All Automations
    const allAutomations = [...simplifiedActionPipelines, ...simplifiedMetadataTests];

    // Get Categories
    const categories = uniq(allAutomations.map((automation: any) => automation.category));

    // Build tabs
    const tabs: any = [
        {
            key: 'all',
            label: 'All',
            data: allAutomations,
        },
    ];
    categories.forEach((category: string) => {
        tabs.push({
            key: category,
            label: category,
            data: allAutomations.filter((automation: any) => automation.category === category),
        });
    });

    const [activeTab, setActiveTab] = useState(tabs[0].key);
    const data = tabs.filter((tab) => tab.key === activeTab)[0].data || [];

    // Data states
    // const isLoading = testsLoading || actionsLoading;
    // const isError = testsError || actionsError;
    // const noData = allAutomations.length === 0;

    // POC Variables
    const hideSidebar = true;

    return (
        <>
            <AutomationsPageContainer>
                {!hideSidebar && (
                    <AutomationsSidebar>
                        <h1>Sidebar</h1>
                    </AutomationsSidebar>
                )}
                <AutomationsContent>
                    <AutomationsContentHeader>
                        <div>
                            <h1>All Automations</h1>
                            <p>Description</p>
                        </div>
                        <div>
                            <LargeButtonPrimary onClick={() => setIsOpen(!isOpen)}>
                                Create an Automation
                            </LargeButtonPrimary>
                        </div>
                    </AutomationsContentHeader>
                    <AutomationsContentBody>
                        <AutomationsContentTabs>
                            {tabs.map((tab) => (
                                <AutomationsContentTab
                                    key={tab.key}
                                    isActive={activeTab === tab.key}
                                    onClick={() => setActiveTab(tab.key)}
                                >
                                    {tab.label}
                                </AutomationsContentTab>
                            ))}
                        </AutomationsContentTabs>
                        <AutomationsBody>
                            {data.map((item) => (
                                <AutomationsListCard key={item.key} automation={item} />
                            ))}
                        </AutomationsBody>
                    </AutomationsContentBody>
                </AutomationsContent>
            </AutomationsPageContainer>
            <AutomationCreateModal isOpen={isOpen} setIsOpen={setIsOpen} />
        </>
    );
};
