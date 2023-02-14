import { FileDoneOutlined, FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect, useState } from 'react';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { Assertion, AssertionResultType } from '../../../../../../types.generated';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useEntityData } from '../../../EntityContext';
import { DatasetAssertionsList } from './DatasetAssertionsList';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { sortAssertions } from './assertionUtils';
import { TestResults } from './TestResults';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';

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

enum ViewType {
    ASSERTIONS,
    TESTS,
}

/**
 * Component used for rendering the Validations Tab on the Dataset Page.
 */
export const ValidationsTab = () => {
    const { urn, entityData } = useEntityData();
    const { data, refetch } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    /**
     * Determines which view should be visible: assertions or tests.
     */
    const [view, setView] = useState(ViewType.ASSERTIONS);

    const assertions =
        (combinedData && combinedData.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion)) ||
        [];
    const filteredAssertions = assertions.filter((assertion) => !removedUrns.includes(assertion.urn));
    const numAssertions = filteredAssertions.length;

    const passingTests = (entityData as any)?.testResults?.passing || [];
    const maybeFailingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = maybeFailingTests.length + passingTests.length;

    useEffect(() => {
        if (totalTests > 0 && numAssertions === 0) {
            setView(ViewType.TESTS);
        } else {
            setView(ViewType.ASSERTIONS);
        }
    }, [totalTests, numAssertions]);

    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort(sortAssertions);

    return (
        <>
            <TabToolbar>
                <div>
                    <Button type="text" disabled={numAssertions === 0} onClick={() => setView(ViewType.ASSERTIONS)}>
                        <FileProtectOutlined />
                        Assertions ({numAssertions})
                    </Button>
                    <Button type="text" disabled={totalTests === 0} onClick={() => setView(ViewType.TESTS)}>
                        <FileDoneOutlined />
                        Tests ({totalTests})
                    </Button>
                </div>
            </TabToolbar>
            {(view === ViewType.ASSERTIONS && (
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
            )) || <TestResults passing={passingTests} failing={maybeFailingTests} />}
        </>
    );
};
