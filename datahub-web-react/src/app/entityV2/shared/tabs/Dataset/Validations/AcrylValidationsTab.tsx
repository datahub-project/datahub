import { Tooltip } from '@components';
import React, { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AcrylAssertionList } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionList';
import { AcrylAssertionSummaryTab } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Summary/AcrylAssertionSummaryTab';
import { DataContractTab } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/DataContractTab';
import { useGetValidationsTab } from '@app/entityV2/shared/tabs/Dataset/Validations/useGetValidationsTab';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useAppConfig } from '@app/useAppConfig';

const TabTitle = styled.span`
    margin-left: 4px;
`;

const TabButton = styled.div<{ selected: boolean; disabled: boolean }>`
    display: flex;
    background-color: ${(props) => (props.selected && '#f1f3fd') || 'none'};
    color: ${(props) => (props.selected ? props.theme.styles['primary-color'] : 'none')};
    align-items: center;
    justify-content: center;
    cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    border-radius: 5px;
    padding: 0px 12px 0px 12px;
    font-size: 14px;
    height: 40px;
    color: ${(props) => (props.disabled && '#00000040') || 'none'};
`;
const TabToolbar = styled.div`
    display: flex;
    position: relative;
    z-index: 1;
    height: 46px;
    padding: 7px 12px;
    flex: 0 0 auto;
`;

const TabContentWrapper = styled.div`
    @media screen and (max-height: 800px) {
        display: contents;
        overflow: auto;
    }
`;

enum TabPaths {
    ASSERTIONS = 'List',
    DATA_CONTRACT = 'Data Contract',
    SUMMARY = 'Summary',
}

const DEFAULT_TAB = TabPaths.SUMMARY;

/**
 * Acryl-specific component used for rendering the Entity Validations Tab.
 */
export const AcrylValidationsTab = () => {
    const history = useHistory();
    const { pathname } = useLocation();
    const { entityData } = useEntityData();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const isRenderingSiblings = (entityData?.siblingsSearch?.total && !isHideSiblingMode) || false;
    const appConfig = useAppConfig();

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
                    <TabTitle>Summary</TabTitle>
                </>
            ),
            path: TabPaths.SUMMARY,
            disabled: false, // Always keep the assertions tab clickable in saas.
            content: <AcrylAssertionSummaryTab />,
            id: 'summary',
        },
        {
            title: (
                <>
                    <TabTitle>Assertions</TabTitle>
                </>
            ),
            path: TabPaths.ASSERTIONS,
            disabled: false, // Always keep the assertions tab clickable in saas.
            content: <AcrylAssertionList />,
            id: 'assertions',
        },
    ];

    if (appConfig.config.featureFlags?.dataContractsEnabled) {
        // If contracts feature is enabled, add to list.
        tabs.push({
            title: (
                <>
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
            id: 'data-contract',
        });
    }

    return (
        <>
            <TabToolbar>
                {tabs.map((tab) => (
                    <Tooltip showArrow={false} title={tab.tip}>
                        <TabButton
                            key={tab.path}
                            disabled={tab.disabled}
                            selected={selectedTab === tab.path}
                            id={`acryl-validation-tab-${tab.id}-sub-tab`}
                            onClick={() => {
                                if (!tab.disabled) {
                                    history.replace(
                                        `${basePath}/${tab.path}?${SEPARATE_SIBLINGS_URL_PARAM}=${isHideSiblingMode}`,
                                    );
                                }
                            }}
                        >
                            {tab.title}
                        </TabButton>
                    </Tooltip>
                ))}
            </TabToolbar>
            <TabContentWrapper>
                {tabs.filter((tab) => tab.path === selectedTab).map((tab) => tab.content)}
            </TabContentWrapper>
        </>
    );
};
