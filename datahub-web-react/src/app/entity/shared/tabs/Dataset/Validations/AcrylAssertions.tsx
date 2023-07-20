import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
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

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertions = () => {
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);

    const { urn, entityData } = useEntityData();
    const { entityType } = useEntityData();
    const { config } = useAppConfig();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const { data, refetch } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const assertions = combinedData?.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion) || [];
    const filteredAssertions = assertions.filter((assertion) => !removedUrns.includes(assertion.urn));
    const assertionGroups = createAssertionGroups(filteredAssertions);

    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;

    return (
        <>
            {assertionMonitorsEnabled && isEntityEligibleForAssertionMonitoring(entityData?.platform?.urn) && (
                <TabToolbar>
                    <Button type="text" onClick={() => setShowAssertionBuilder(true)}>
                        <PlusOutlined /> Create Assertion
                    </Button>
                </TabToolbar>
            )}
            <DatasetAssertionsSummary summary={getAssertionGroupSummary(filteredAssertions)} />
            <AssertionGroupTable
                groups={assertionGroups}
                onDeletedAssertion={(assertionUrn) => {
                    // Hack to deal with eventual consistency.
                    setRemovedUrns([...removedUrns, assertionUrn]);
                    setTimeout(() => refetch(), 3000);
                }}
                onUpdatedAssertion={() => {
                    setTimeout(() => refetch(), 3000);
                }}
            />
            {showAssertionBuilder && (
                <AssertionMonitorBuilderModal
                    entityUrn={urn}
                    entityType={entityType}
                    platformUrn={entityData?.platform?.urn as string}
                    onSubmit={() => {
                        setShowAssertionBuilder(false);
                        // TODO: Use the Apollo Cache.
                        setTimeout(() => refetch(), 3000);
                    }}
                    onCancel={() => setShowAssertionBuilder(false)}
                />
            )}
        </>
    );
};
