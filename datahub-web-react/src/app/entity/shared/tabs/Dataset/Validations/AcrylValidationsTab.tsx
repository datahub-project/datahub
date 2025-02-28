import React, { useEffect } from 'react';
import { Button } from 'antd';
import { Tooltip } from '@components';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { AuditOutlined, FileProtectOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useGetValidationsTab } from './useGetValidationsTab';
import { ANTD_GRAY } from '../../../constants';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { AcrylAssertions } from './AcrylAssertions';
import { useAppConfig } from '../../../../../useAppConfig';
import { DataContractTab } from './contract/DataContractTab';
import {
    SEPARATE_SIBLINGS_URL_PARAM,
    combineEntityDataWithSiblings,
    useIsSeparateSiblingsMode,
} from '../../../siblingUtils';

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
 * Acryl-specific component used for rendering the Entity Validations Tab.
 */
export const AcrylValidationsTab = () => {
    const history = useHistory();
    const { pathname } = useLocation();
    const { urn, entityData } = useEntityData();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const isRenderingSiblings = (entityData?.siblingsSearch?.total && !isHideSiblingMode) || false;
    const appConfig = useAppConfig();

    const { data: assertionsData } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const combinedData = isHideSiblingMode ? assertionsData : combineEntityDataWithSiblings(assertionsData);
    const totalAssertions = combinedData?.dataset?.assertions?.assertions?.length || 0;

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
    const tabs: any[] = [
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
            disabled: isRenderingSiblings,
            tip: isRenderingSiblings ? (
                <>
                    You cannot view a data contract for a group of assets. <br />
                    <br />
                    To view the data contract for a specific asset in this group, navigate to them using the{' '}
                    <b>Composed Of</b> sidebar section on the right.
                </>
            ) : null,
        });
    }

    return (
        <>
            <TabToolbar>
                <div>
                    {tabs.map((tab) => (
                        <Tooltip showArrow={false} title={tab.tip}>
                            <TabButton
                                key={tab.path}
                                type="text"
                                disabled={tab.disabled}
                                selected={selectedTab === tab.path}
                                onClick={() =>
                                    history.replace(
                                        `${basePath}/${tab.path}?${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}`,
                                    )
                                }
                            >
                                {tab.title}
                            </TabButton>
                        </Tooltip>
                    ))}
                </div>
            </TabToolbar>
            {tabs.filter((tab) => tab.path === selectedTab).map((tab) => tab.content)}
        </>
    );
};
