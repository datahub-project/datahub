import { AuditOutlined, FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { Assertions } from '@app/entity/shared/tabs/Dataset/Validations/Assertions';
import { DataContractTab } from '@app/entity/shared/tabs/Dataset/Validations/contract/DataContractTab';
import { useGetValidationsTab } from '@app/entity/shared/tabs/Dataset/Validations/useGetValidationsTab';
import { useAppConfig } from '@app/useAppConfig';

const TabTitle = styled.span`
    margin-left: 4px;
`;

const TabButton = styled(Button)<{ selected: boolean }>`
    background-color: ${(props) => (props.selected && ANTD_GRAY[3]) || 'none'};
    margin-left: 4px;
`;

enum TabPaths {
    ASSERTIONS = 'List',
    DATA_CONTRACT = 'Data Contract',
}

const DEFAULT_TAB = TabPaths.ASSERTIONS;

/**
 * Component used for rendering the Entity Validations Tab.
 */
export const ValidationsTab = () => {
    const { entityData } = useEntityData();
    const history = useHistory();
    const { pathname } = useLocation();
    const appConfig = useAppConfig();

    const totalAssertions = (entityData as any)?.assertions?.total;

    const { selectedTab, basePath } = useGetValidationsTab(pathname, Object.values(TabPaths));

    // If no tab was selected, select a default tab.
    useEffect(() => {
        if (!selectedTab) {
            // Route to the default tab.
            history.replace(`${basePath}/${DEFAULT_TAB}`);
        }
    }, [selectedTab, basePath, history]);

    /**
     * The top-level Toolbar tabs to display.
     */
    const tabs = [
        {
            title: (
                <>
                    <FileProtectOutlined />
                    <TabTitle>Assertions ({totalAssertions})</TabTitle>
                </>
            ),
            path: TabPaths.ASSERTIONS,
            disabled: totalAssertions === 0,
            content: <Assertions />,
        },
    ];

    if (appConfig.config.featureFlags?.dataContractsEnabled) {
        // If contracts feature is enabled, add to list.
        tabs.push({
            title: (
                <>
                    <AuditOutlined />

                    <TabTitle>Data Contract</TabTitle>
                </>
            ),
            path: TabPaths.DATA_CONTRACT,
            content: <DataContractTab />,
            disabled: false,
        });
    }

    return (
        <>
            <TabToolbar>
                <div>
                    {tabs.map((tab) => (
                        <TabButton
                            type="text"
                            disabled={tab.disabled}
                            selected={selectedTab === tab.path}
                            onClick={() => history.replace(`${basePath}/${tab.path}`)}
                        >
                            {tab.title}
                        </TabButton>
                    ))}
                </div>
            </TabToolbar>
            {tabs.filter((tab) => tab.path === selectedTab).map((tab) => tab.content)}
        </>
    );
};
