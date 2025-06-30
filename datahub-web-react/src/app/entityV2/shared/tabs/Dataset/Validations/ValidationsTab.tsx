import { FileDoneOutlined, FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { TestResults } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResults';
import { Assertions } from '@app/entityV2/shared/tabs/Dataset/Validations/Assertions';
import { useGetValidationsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/useGetValidationsTab';

const TabTitle = styled.span`
    margin-left: 4px;
`;

const TabButton = styled(Button)<{ selected: boolean }>`
    background-color: ${(props) => (props.selected && ANTD_GRAY[3]) || 'none'};
    margin-left: 4px;
`;

enum TabPaths {
    ASSERTIONS = 'Assertions',
    TESTS = 'Tests',
}

const DEFAULT_TAB = TabPaths.ASSERTIONS;

/**
 * Component used for rendering the Entity Validations Tab.
 */

// CAN DELETE
export const ValidationsTab = () => {
    const { entityData } = useEntityData();
    const history = useHistory();
    const { pathname } = useLocation();

    const totalAssertions = (entityData as any)?.assertions?.total;
    const passingTests = (entityData as any)?.testResults?.passing || [];
    const maybeFailingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = maybeFailingTests.length + passingTests.length;

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
            disabled: false,
            content: <Assertions />,
        },
        {
            title: (
                <>
                    <FileDoneOutlined />
                    <TabTitle>Tests ({totalTests})</TabTitle>
                </>
            ),
            path: TabPaths.TESTS,
            disabled: totalTests === 0,
            content: <TestResults passing={passingTests} failing={maybeFailingTests} />,
        },
    ];

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
