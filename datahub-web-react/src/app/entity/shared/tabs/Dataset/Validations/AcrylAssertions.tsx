import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
import { useConnectionForEntityExistsQuery } from '../../../../../../graphql/connection.generated';
import { getPlatformName } from '../../../utils';
import { Assertion } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderModal } from './assertion/builder/AssertionMonitorBuilderModal';
import TabToolbar from '../../../components/styled/TabToolbar';
import { isEntityEligibleForAssertionMonitoring } from './assertion/builder/utils';
import { createAssertionGroups, getAssertionGroupSummary } from './acrylUtils';
import { AssertionGroupTable } from './AssertionGroupTable';
import {
    updateDatasetAssertionsCache,
    removeFromDatasetAssertionsCache,
    createCachedAssertionWithMonitor,
} from './acrylCacheUtils';

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
    const { data: connectionExistsData } = useConnectionForEntityExistsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const assertions = combinedData?.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion) || [];
    const assertionGroups = createAssertionGroups(assertions);
    const platformName = getPlatformName(entityData);

    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;
    return (
        <>
            {assertionMonitorsEnabled && isEntityEligibleForAssertionMonitoring(entityData?.platform?.urn) && (
                <TabToolbar>
                    <Tooltip
                        title={
                            !connectionExistsData?.connectionForEntityExists
                                ? `A connection to ${platformName} is required to create & run assertions. Configure your connection inside Ingestion, or contact your DataHub admin for help.`
                                : undefined
                        }
                        placement="right"
                    >
                        <Button
                            type="text"
                            onClick={() => setShowAssertionBuilder(true)}
                            disabled={!connectionExistsData?.connectionForEntityExists}
                        >
                            <PlusOutlined /> Create Assertion
                        </Button>
                    </Tooltip>
                </TabToolbar>
            )}
            <DatasetAssertionsSummary summary={getAssertionGroupSummary(assertions)} />
            <AssertionGroupTable
                groups={assertionGroups}
                onDeletedAssertion={(assertionUrn) => {
                    removeFromDatasetAssertionsCache(urn, assertionUrn, client);
                    setTimeout(() => refetch(), 5000);
                }}
                onUpdatedAssertion={(assertion) => {
                    updateDatasetAssertionsCache(urn, assertion, client);
                    setTimeout(() => refetch(), 5000);
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
