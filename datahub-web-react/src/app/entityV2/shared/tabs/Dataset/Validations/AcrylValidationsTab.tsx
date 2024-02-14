import React, { useEffect } from 'react';
import { Button } from 'antd';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { AuditOutlined, FileDoneOutlined, FileProtectOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import { AcrylTestResults } from './AcrylTestResults';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useGetValidationsTab } from './useGetValidationsTab';
import { ANTD_GRAY } from '../../../constants';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { AssertionSourceType } from '../../../../../../types.generated';
import { AcrylAssertions } from './AcrylAssertions';
import { useAppConfig } from '../../../../../useAppConfig';
import { DataContractTab } from './contract/DataContractTab';

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
    DATA_CONTRACT = 'Data Contract',
}

const DEFAULT_TAB = TabPaths.ASSERTIONS;

/**
 * Acryl-specific component used for rendering the Entity Validations Tab.
 */
export const AcrylValidationsTab = () => {
    const history = useHistory();
    const { pathname } = useLocation();
    const { urn, entityData } = useEntityData();
    const appConfig = useAppConfig();

    const { data: assertionsData } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const totalAssertions =
        assertionsData?.dataset?.assertions?.assertions?.filter(
            // SaaS-Only filtering.
            (assertion) => assertion.info?.source?.type !== AssertionSourceType.Inferred,
        ).length || 0;

    const passingTests = (entityData as any)?.testResults?.passing || [];
    const failingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = failingTests.length + passingTests.length;

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
            disabled: false, // Always keep the assertions tab clickable in saas.
            content: <AcrylAssertions />,
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
            content: <AcrylTestResults urn={urn} />,
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
