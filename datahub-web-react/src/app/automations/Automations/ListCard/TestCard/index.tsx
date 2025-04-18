import React from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';

import { formatNumberWithoutAbbreviation } from '../../../../shared/formatNumber';
import { toRelativeTimeString } from '../../../../shared/time/timeUtils';
import { useGetTestResultsSummaryQuery, useDeleteTestMutation } from '../../../../../graphql/test.generated';

import { truncateString } from '../../../utils';

import { ListCardHeader, ListCardBody, StyledDivider, Category, Details, ResultContainer } from '../../components';

import { ActionsMenu } from '../ActionsMenu';

dayjs.extend(localizedFormat);

const TestDetails = ({ urn }: any) => {
    const { data: results } = useGetTestResultsSummaryQuery({
        skip: !urn,
        variables: {
            urn,
        },
    });

    const passingCount =
        results?.test?.results?.passingCount !== undefined
            ? formatNumberWithoutAbbreviation(results?.test?.results?.passingCount)
            : '-';

    const failingCount =
        results?.test?.results?.failingCount !== undefined
            ? formatNumberWithoutAbbreviation(results?.test?.results?.failingCount)
            : '-';

    const lastComputed =
        results?.test?.results?.lastRunTimestampMillis !== undefined
            ? toRelativeTimeString(results?.test?.results?.lastRunTimestampMillis || 0)
            : 'unknown';

    return (
        <ResultContainer>
            <div>
                <span className="pass">Passing: {passingCount}</span> <br />
                <span className="fail">Failing: {failingCount}</span>
            </div>
            <div style={{ textAlign: 'right' }}>Last Computed {lastComputed}</div>
        </ResultContainer>
    );
};

interface TestCardProps {
    automation: any;
    openEditModal: () => void;
}

export const TestCard = ({ automation, openEditModal }: TestCardProps) => {
    const { urn, category, description, name } = automation;

    const [deleteTestMutation] = useDeleteTestMutation();
    const deleteTest = () => {
        deleteTestMutation({ variables: { urn } });
    };

    return (
        <>
            <ListCardHeader>
                {category && (
                    <div className="categoryAndDeployed">
                        <Category>{category.toString().toUpperCase()}</Category>
                        <ActionsMenu
                            items={[
                                {
                                    key: 'edit',
                                    onClick: openEditModal,
                                    disabled: false,
                                    icon: 'Edit',
                                    label: 'Edit Automation',
                                    tooltip: 'Edit the automation configuration.',
                                },
                                {
                                    key: 'delete',
                                    onClick: deleteTest,
                                    disabled: false,
                                    icon: 'Delete',
                                    label: 'Delete Automation',
                                    tooltip: 'Delete the automation.',
                                },
                            ]}
                        />
                    </div>
                )}
                {name && (
                    <div className="titleAndButtons">
                        <h2>{name}</h2>
                    </div>
                )}
            </ListCardHeader>
            <StyledDivider />
            <ListCardBody>
                {description && (
                    <div className="description">
                        <p>{truncateString(description, 125)}</p>
                    </div>
                )}
                <Details>
                    <TestDetails urn={urn} />
                </Details>
            </ListCardBody>
        </>
    );
};
