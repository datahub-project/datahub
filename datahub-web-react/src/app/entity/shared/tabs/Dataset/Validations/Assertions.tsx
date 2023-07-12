import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { Assertion, AssertionResultType, AssertionSourceType } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { DatasetAssertionsList } from './DatasetAssertionsList';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { sortAssertions } from './assertionUtils';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderModal } from './assertion/builder/AssertionMonitorBuilderModal';
import TabToolbar from '../../../components/styled/TabToolbar';
import { isEntityEligibleForAssertionMonitoring } from './assertion/builder/utils';

/**
 * Returns a status summary for the assertions associated with a Dataset.
 */
const getAssertionsStatusSummary = (assertions: Array<Assertion>) => {
    const summary = {
        failedRuns: 0,
        succeededRuns: 0,
        totalRuns: 0,
        totalAssertions: assertions.length,
    };
    assertions.forEach((assertion) => {
        if ((assertion.runEvents?.runEvents?.length || 0) > 0) {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            if (AssertionResultType.Success === resultType) {
                summary.succeededRuns++;
            }
            if (AssertionResultType.Failure === resultType) {
                summary.failedRuns++;
            }
            summary.totalRuns++; // only count assertions for which there is one completed run event!
        }
    });
    return summary;
};

/**
 * Component used for rendering the Validations Tab on the Dataset Page.
 */
export const Assertions = () => {
    const { urn, entityData } = useEntityData();
    const { data, refetch } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    // Start SaaS only
    const { entityType } = useEntityData();
    const { config } = useAppConfig();
    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;
    // End SaaS only

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    // Start saas only
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);
    // End saas only

    const assertions =
        (combinedData && combinedData.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion)) ||
        [];
    const filteredAssertions = assertions.filter(
        (assertion) =>
            !removedUrns.includes(assertion.urn) && assertion?.info?.source?.type !== AssertionSourceType.Inferred,
    );

    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort(sortAssertions);

    return (
        <>
            {assertionMonitorsEnabled && isEntityEligibleForAssertionMonitoring(entityData?.platform?.urn) && (
                <TabToolbar>
                    <Button type="text" onClick={() => setShowAssertionBuilder(true)}>
                        <PlusOutlined /> Create Assertion
                    </Button>
                </TabToolbar>
            )}
            <DatasetAssertionsSummary summary={getAssertionsStatusSummary(filteredAssertions)} />
            {entityData && (
                <DatasetAssertionsList
                    assertions={filteredAssertions}
                    onDelete={(assertionUrn) => {
                        // Hack to deal with eventual consistency.
                        setRemovedUrns([...removedUrns, assertionUrn]);
                        setTimeout(() => refetch(), 3000);
                    }}
                    onUpdate={() => {
                        setTimeout(() => refetch(), 3000);
                    }}
                />
            )}
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
