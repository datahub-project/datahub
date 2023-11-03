import React from 'react';
import { Collapse, Form } from 'antd';
import styled from 'styled-components';
import {
    Assertion,
    AssertionType,
    DatasetFilter,
    DatasetFreshnessAssertionParameters,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
    Monitor,
} from '../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../steps/freshness/EvaluationScheduleBuilder';
import { DatasetFreshnessScheduleBuilder } from '../steps/freshness/DatasetFreshnessScheduleBuilder';
import { DatasetFreshnessSourceBuilder } from '../steps/freshness/DatasetFreshnessSourceBuilder';
import { DatasetFreshnessFilterBuilder } from '../steps/freshness/DatasetFreshnessFilterBuilder';
import { useEntityData } from '../../../../../../EntityContext';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    assertion: Assertion;
};

/**
 * This component is used to view Freshness assertion details in read-only mode.
 */
export const FreshnessAssertionDetails = ({ assertion }: Props) => {
    const { urn, entityData } = useEntityData();
    const freshnessAssertion = assertion?.info?.freshnessAssertion;
    const freshnessFilter = freshnessAssertion?.filter;
    const freshnessSchedule = freshnessAssertion?.schedule;
    const freshnessScheduleType = freshnessSchedule?.type;
    const monitor = (assertion as any)?.monitor?.relationships?.[0]?.entity as Monitor;
    const schedule = monitor?.info?.assertionMonitor?.assertions?.[0]?.schedule;
    const parameters = monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters;
    const datasetFreshnessParameters = parameters?.datasetFreshnessParameters;
    const platformUrn = entityData?.platform?.urn as string;

    return (
        <Form
            initialValues={{
                column: datasetFreshnessParameters?.field?.path,
            }}
        >
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.Freshness}
                onChange={() => {}}
                disabled
            />
            <DatasetFreshnessScheduleBuilder
                value={freshnessSchedule as FreshnessAssertionSchedule}
                onChange={() => {}}
                disabled
            />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <DatasetFreshnessSourceBuilder
                            entityUrn={urn}
                            platformUrn={platformUrn}
                            scheduleType={freshnessScheduleType as FreshnessAssertionScheduleType}
                            value={datasetFreshnessParameters as DatasetFreshnessAssertionParameters}
                            onChange={() => {}}
                            disabled
                        />
                        <DatasetFreshnessFilterBuilder
                            value={freshnessFilter as DatasetFilter}
                            sourceType={datasetFreshnessParameters?.sourceType}
                            onChange={() => {}}
                            disabled
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
        </Form>
    );
};
