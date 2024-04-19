import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderDrawer } from './assertion/builder/AssertionMonitorBuilderDrawer';
import TabToolbar from '../../../components/styled/TabToolbar';
import { AssertionWithMonitorDetails, createAssertionGroups, getLegacyAssertionsSummary, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from './acrylUtils';
import { AssertionGroupTable } from './AssertionGroupTable';
import { updateDatasetAssertionsCache, createCachedAssertionWithMonitor } from './acrylCacheUtils';
import { useGetDatasetContractQuery } from '../../../../../../graphql/contract.generated';
import { combineEntityDataWithSiblings } from '../../../../../entity/shared/siblingUtils';

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
    const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] = tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
    const assertionGroups = createAssertionGroups(assertionsWithMonitorsDetails);

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
                        showArrow={false}
                        title={
                            !canCreateAssertion && 'You do not have permission to create an assertion for this asset'
                        }
                    >
                        <Button
                            type="text"
                            onClick={() => canCreateAssertion && setShowAssertionBuilder(true)}
                            disabled={!canCreateAssertion}
                            id="create-assertion-btn-main"
                        >
                            <PlusOutlined /> Create
                        </Button>
                    </Tooltip>
                </TabToolbar>
            )}
            <DatasetAssertionsSummary summary={getLegacyAssertionsSummary(assertionsWithMonitorsDetails)} />
            <AssertionGroupTable
                groups={assertionGroups}
                contract={contract}
                refetch={() => {
                    refetch();
                    contractRefetch();
                }}
                canEditAssertions={data?.dataset?.privileges?.canEditAssertions || false}
                canEditMonitors={data?.dataset?.privileges?.canEditMonitors || false}
                canEditSqlAssertions={data?.dataset?.privileges?.canEditSqlAssertionMonitors || false}
            />
            {showAssertionBuilder && (
                <AssertionMonitorBuilderDrawer
                    entityUrn={urn}
                    entityType={entityType}
                    platformUrn={entityData?.platform?.urn as string}
                    onSubmit={(assertion) => {
                        setShowAssertionBuilder(false);
                        updateDatasetAssertionsCache(urn, createCachedAssertionWithMonitor(assertion), client);
                        setTimeout(() => refetch(), 5000);
                    }}
                    onCancel={() => setShowAssertionBuilder(false)}
                />
            )}
        </>
    );
};
