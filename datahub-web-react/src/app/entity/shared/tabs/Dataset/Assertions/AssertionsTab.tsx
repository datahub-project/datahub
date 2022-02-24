import { FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { Assertion, AssertionResultType } from '../../../../../../types.generated';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useEntityData } from '../../../EntityContext';
import { AssertionsList } from './AssertionsList';
import { AssertionsSummaryHeader } from './AssertionsSummary';

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
        if (assertion.runEvents?.runEvents.length) {
            const mostRecentRun = assertion.runEvents?.runEvents[0];
            const resultType = mostRecentRun.result?.type;
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
export const AssertionTab = () => {
    const { urn, entityData } = useEntityData();
    const { data } = useGetDatasetAssertionsQuery({ variables: { urn } });

    const assertions = (data && data.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion)) || [];
    const totalAssertions = data?.dataset?.assertions?.total || 0;

    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort((a, b) => {
        if (!a.runEvents?.runEvents.length) {
            return 1;
        }
        if (!b.runEvents?.runEvents.length) {
            return -1;
        }
        return b.runEvents.runEvents[0].timestampMillis - a.runEvents.runEvents[0].timestampMillis;
    });

    return (
        <>
            <TabToolbar>
                <div>
                    <Button type="text">
                        <FileProtectOutlined />
                        Assertions ({totalAssertions})
                    </Button>
                </div>
            </TabToolbar>
            <AssertionsSummaryHeader summary={getAssertionsStatusSummary(assertions)} />
            {entityData && <AssertionsList assertions={assertions} />}
        </>
    );
};
