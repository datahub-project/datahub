import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { DescriptionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/DescriptionBuilder';
import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { SqlEvaluationBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlEvaluationBuilder';
import { SqlQueryBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlQueryBuilder';
import { nullsToUndefined } from '@src/app/entityV2/shared/utils';

import { Assertion, AssertionType, Monitor } from '@types';

const Section = styled.div`
    margin-bottom: 24px;
`;

type Props = {
    assertion: Assertion;
};

/**
 * This component is used to view SQL assertion details in read-only mode.
 */
export const SqlAssertionDetails = ({ assertion }: Props) => {
    const sqlAssertion = assertion?.info?.sqlAssertion;
    const monitor = (assertion as any)?.monitor?.relationships?.[0]?.entity as Monitor;
    const schedule = monitor?.info?.assertionMonitor?.assertions?.[0]?.schedule;

    return (
        <Form
            initialValues={{
                query: sqlAssertion?.statement,
                description: assertion?.info?.description,
                sqlParameters: {
                    value: sqlAssertion?.parameters?.value?.value,
                    minValue: sqlAssertion?.parameters?.minValue?.value,
                    maxValue: sqlAssertion?.parameters?.maxValue?.value,
                },
            }}
        >
            <Section>
                <EvaluationScheduleBuilder
                    value={schedule}
                    assertionType={AssertionType.Sql}
                    showAdvanced={false}
                    onChange={() => {}}
                    disabled
                />
                <SqlQueryBuilder value={sqlAssertion?.statement} onChange={() => {}} disabled />
                <SqlEvaluationBuilder
                    value={{
                        assertion: {
                            sqlAssertion: {
                                type: sqlAssertion?.type,
                                operator: sqlAssertion?.operator,
                                changeType: sqlAssertion?.changeType ?? undefined,
                                parameters: nullsToUndefined(sqlAssertion?.parameters),
                            },
                        },
                    }}
                    onChange={() => {}}
                    disabled
                />
                <DescriptionBuilder value={assertion?.info?.description} onChange={() => {}} disabled />
            </Section>
        </Form>
    );
};
