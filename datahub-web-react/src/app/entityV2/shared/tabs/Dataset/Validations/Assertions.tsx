import React, { useState } from 'react';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { Assertion, AssertionResultType } from '../../../../../../types.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '../../../../../entity/shared/siblingUtils';
import { DatasetAssertionsList } from './DatasetAssertionsList';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { sortAssertions } from './assertionUtils';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';

/**
 * Returns a status summary for the assertions associated with a Dataset.
 */
const getAssertionsStatusSummary = (assertions: Array<Assertion>) => {
    const summary = {
        failedRuns: 0,
        succeededRuns: 0,
        erroredRuns: 0,
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
            if (AssertionResultType.Error === resultType) {
                summary.erroredRuns++;
            }
            if (AssertionResultType.Init !== resultType) {
                summary.totalRuns++; // only count assertions for which there is one completed run event, ignoring INIT statuses!
            }
        }
    });
    return summary;
};

/**
 * Component used for rendering the Validations Tab on the Dataset Page.
 *
 * TODO: Note that only the legacy DATASET assertions are supported for viewing as of today.
 */
// CAN DELETE
export const Assertions = () => {
    const { urn, entityData } = useEntityData();
    const { data, refetch } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const assertions =
        (combinedData && combinedData.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion)) ||
        [];
    const filteredAssertions = assertions.filter(
        (assertion) => !removedUrns.includes(assertion.urn) && !!assertion.info?.datasetAssertion,
    );

    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort(sortAssertions);

    return (
        <>
            <DatasetAssertionsSummary summary={getAssertionsStatusSummary(filteredAssertions)} />
            {entityData && (
                <DatasetAssertionsList
                    assertions={filteredAssertions}
                    onDelete={(assertionUrn) => {
                        // Hack to deal with eventual consistency.
                        setRemovedUrns([...removedUrns, assertionUrn]);
                        setTimeout(() => refetch(), 3000);
                    }}
                />
            )}
        </>
    );
};
