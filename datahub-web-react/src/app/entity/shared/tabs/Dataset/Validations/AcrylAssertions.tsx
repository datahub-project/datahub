import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Tooltip } from '@components';
import { TableLoadingSkeleton } from '@src/app/entityV2/shared/TableLoadingSkeleton';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../EntityContext';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderDrawer } from './assertion/builder/AssertionMonitorBuilderDrawer';
import TabToolbar from '../../../components/styled/TabToolbar';
import {
    AssertionWithMonitorDetails,
    createAssertionGroups,
    getLegacyAssertionsSummary,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from './acrylUtils';
import { AssertionGroupTable } from './AssertionGroupTable';
import { updateDatasetAssertionsCache, createCachedAssertionWithMonitor } from './acrylCacheUtils';
import { useGetDatasetContractQuery } from '../../../../../../graphql/contract.generated';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertions = () => {
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);

    const { urn, entityData } = useEntityData();
    const { entityType } = useEntityData();
    const { config } = useAppConfig();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const { data, refetch, client, loading } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
        tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
    const assertionGroups = createAssertionGroups(assertionsWithMonitorsDetails);

    const contract = contractData?.dataset?.contract as any;
    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;

    const isSiblingMode = (entityData?.siblingsSearch?.total && !isHideSiblingMode) || false;
    const isSiblingModeMessage = (
        <>
            You cannot create an assertion for a group of assets. <br />
            <br />
            To create an assertion for a specific asset in this group, navigate to them using the <b>
                Composed Of
            </b>{' '}
            sidebar section below.
        </>
    );

    const isAllowedToCreateAssertion =
        (data?.dataset?.privileges?.canEditAssertions || false) &&
        (data?.dataset?.privileges?.canEditMonitors || false);
    const isNotAllowedToCreateAssertionMessage = 'You do not have permission to create an assertion for this asset';

    /* We do not enable the create button if the user does not have the privilege, OR if sibling mode is enabled */
    const disableCreateAssertion = !isAllowedToCreateAssertion || isSiblingMode;
    const disableCreateAssertionMessage = isSiblingMode ? isSiblingModeMessage : isNotAllowedToCreateAssertionMessage;

    return (
        <>
            {assertionMonitorsEnabled && (
                <TabToolbar>
                    <Tooltip
                        showArrow={false}
                        title={(disableCreateAssertion && disableCreateAssertionMessage) || null}
                    >
                        <Button
                            type="text"
                            onClick={() => !disableCreateAssertion && setShowAssertionBuilder(true)}
                            disabled={disableCreateAssertion}
                            id="create-assertion-btn-main"
                        >
                            <PlusOutlined /> Create
                        </Button>
                    </Tooltip>
                </TabToolbar>
            )}
            {loading ? (
                <TableLoadingSkeleton />
            ) : (
                <>
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
                </>
            )}
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
