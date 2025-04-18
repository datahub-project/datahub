import React, { useEffect } from 'react';
import { Button } from 'antd';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { FileDoneOutlined } from '@ant-design/icons';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import TabToolbar from '../../../components/styled/TabToolbar';
import { ANTD_GRAY } from '../../../constants';
import { AcrylTestResults } from './AcrylTestResults';
import { useGetValidationsTab } from '../Validations/useGetValidationsTab';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';

const TabTitle = styled.span`
    margin-left: 4px;
`;

const TabButton = styled(Button)<{ selected: boolean }>`
    background-color: ${(props) => (props.selected && ANTD_GRAY[3]) || 'none'};
    margin-left: 4px;
`;

enum TabPaths {
    TESTS = 'Tests',
}

const DEFAULT_TAB = TabPaths.TESTS;

export const GovernanceTab = () => {
    const history = useHistory();
    const { pathname } = useLocation();
    const { urn, entityData } = useEntityData();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const passingTests = (entityData as any)?.testResults?.passing || [];
    const failingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = failingTests.length + passingTests.length;
    const { selectedTab, basePath } = useGetValidationsTab(pathname, Object.values(TabPaths));

    // If no tab was selected, select a default tab.
    useEffect(() => {
        if (!selectedTab) {
            // Route to the default tab.
            history.replace(`${basePath}/${DEFAULT_TAB}?${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}`);
        }
    }, [selectedTab, basePath, history, isHideSiblingMode]);

    /**
     * The top-level Toolbar tabs to display.
     */
    const tabs = [
        {
            title: (
                <>
                    <FileDoneOutlined />
                    <TabTitle>Tests ({totalTests})</TabTitle>
                </>
            ),
            path: TabPaths.TESTS,
            disabled: totalTests === 0,
            content: <AcrylTestResults urn={urn} />,
        },
    ];

    return (
        <>
            <TabToolbar>
                <div>
                    {tabs.map((tab) => (
                        <TabButton
                            key={tab.path}
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
