import React, { useEffect, useState } from 'react';
import { Button } from 'antd';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { AuditOutlined, FileProtectOutlined, FileDoneOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import { Assertions } from './Assertions';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useGetValidationsTab } from './useGetValidationsTab';
import { ANTD_GRAY } from '../../../constants';
import { useAppConfig } from '../../../../../useAppConfig';
import { DataContractTab } from './contract/DataContractTab';
import { Metrics } from './Metrics';
import { DataQuality } from '../../../../../../types.generated';

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
    METRICS = 'Metrics',
}

//const DEFAULT_TAB = TabPaths.ASSERTIONS;

/**
 * Component used for rendering the Entity Validations Tab.
 */
export const ValidationsTab = () => {
    const { entityData, loading } = useEntityData();
    const history = useHistory();
    const { pathname } = useLocation();
    const appConfig = useAppConfig();

    const { selectedTab, basePath } = useGetValidationsTab(pathname, Object.values(TabPaths));
    const [totalAssertions, setTotalAssertions] = useState(0);
    const [dataQuality, setDataQuality] = useState({});
    const [qualityMetrics, setQualityMetrics] = useState(0);

    let defaultTab = '';
    // If no tab was selected, select a default tab.
    useEffect(() => {
        setTotalAssertions((entityData as any)?.assertions?.total || 0);
        const quality = (entityData as any)?.dataQuality || [];
        setDataQuality(quality);
        setQualityMetrics(Object.keys(quality)?.length || 0);

        if (totalAssertions !== 0) {
            defaultTab = TabPaths.ASSERTIONS;
        } else if (qualityMetrics > 0) {
            defaultTab = TabPaths.METRICS;
        }
        // If no tab was selected, select a default tab.
        if (!selectedTab) {
            // Route to the default tab.s
            history.replace(`${basePath}/${defaultTab}`);
        }
    }, [selectedTab, basePath, history, entityData, loading]);

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
        {
            title: (
                <>
                    <FileDoneOutlined />
                    <TabTitle>Metrics </TabTitle>
                </>
            ),
            path: TabPaths.METRICS,
            disabled: qualityMetrics === 0,
            content: <Metrics metrics={dataQuality} />,
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
