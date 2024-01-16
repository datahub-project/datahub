import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
import { Assertion } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderModal } from './assertion/builder/AssertionMonitorBuilderModal';
import TabToolbar from '../../../components/styled/TabToolbar';
import { createAssertionGroups, getLegacyAssertionsSummary } from './acrylUtils';
import { AssertionGroupTable } from './AssertionGroupTable';
import {
    updateDatasetAssertionsCache,
    removeFromDatasetAssertionsCache,
    createCachedAssertionWithMonitor,
} from './acrylCacheUtils';
import { useGetDatasetContractQuery } from '../../../../../../graphql/contract.generated';
/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertions = () => {
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);

    const { urn, entityData } = useEntityData();
    const { entityType } = useEntityData();
    const { config } = useAppConfig();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const { data, refetch, client } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const assertions = combinedData?.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion) || [];
    const assertionGroups = createAssertionGroups(assertions);

    const contract = contractData?.dataset?.contract as any;
    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;

    const canCreateAssertion =
        (data?.dataset?.privileges?.canEditAssertions || false) &&
        (data?.dataset?.privileges?.canEditMonitors || false);
    return (
        <>
            {assertionMonitorsEnabled && (
                <TabToolbar>
                    <Tooltip
                        title={
                            !canCreateAssertion && 'You do not have permission to create an assertion for this asset'
                        }
                    >
                        <Button
                            type="text"
                            onClick={() => canCreateAssertion && setShowAssertionBuilder(true)}
                            disabled={!canCreateAssertion}
                        >
                            <PlusOutlined /> Create Assertion
                        </Button>
                    </Tooltip>
                </TabToolbar>
            )}
            <DatasetAssertionsSummary summary={getLegacyAssertionsSummary(assertions)} />
            <AssertionGroupTable
                groups={assertionGroups}
                contract={contract}
                onDeletedAssertion={(assertionUrn) => {
                    removeFromDatasetAssertionsCache(urn, assertionUrn, client);
                    setTimeout(() => refetch(), 5000);
                    contractRefetch();
                }}
                onUpdatedAssertion={(assertion) => {
                    updateDatasetAssertionsCache(urn, assertion, client);
                    setTimeout(() => refetch(), 5000);
                    contractRefetch();
                }}
            />
            {showAssertionBuilder && (
                <AssertionMonitorBuilderModal
                    entityUrn={urn}
                    entityType={entityType}
                    platformUrn={entityData?.platform?.urn as string}
                    onSubmit={(assertion, monitor) => {
                        setShowAssertionBuilder(false);
                        updateDatasetAssertionsCache(urn, createCachedAssertionWithMonitor(assertion, monitor), client);
                        setTimeout(() => refetch(), 5000);
                    }}
                    onCancel={() => setShowAssertionBuilder(false)}
                />
            )}
        </>
    );
};