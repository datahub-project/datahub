import React from 'react';
import { render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import SnapshotStatsView from '../stats/snapshot/SnapshotStatsView';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { completeSampleProfile, missingFieldStatsProfile, missingTableStatsProfile } from '../stories/stats';
import { mocks } from '../../../../../Mocks';

describe('SnapshotStatsView', () => {
    it('renders complete profile', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <SnapshotStatsView profile={completeSampleProfile} />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Row Count
        expect(getByText('1000')).toBeInTheDocument();
        expect(getByText('Rows')).toBeInTheDocument();

        // Column Count
        expect(getByText('2000')).toBeInTheDocument();
        expect(getByText('Columns')).toBeInTheDocument();

        // Field Profiles
        // First column
        expect(getByText('testColumn')).toBeInTheDocument();
        expect(getByText('1')).toBeInTheDocument();
        expect(getByText('11.10%')).toBeInTheDocument();
        expect(getByText('2')).toBeInTheDocument();
        expect(getByText('22.20%')).toBeInTheDocument();
        expect(getByText('3')).toBeInTheDocument();
        expect(getByText('4')).toBeInTheDocument();
        expect(getByText('5')).toBeInTheDocument();
        expect(getByText('6')).toBeInTheDocument();
        expect(getByText('value1')).toBeInTheDocument();
        expect(getByText('value2')).toBeInTheDocument();
        expect(getByText('value3')).toBeInTheDocument();

        // Second column
        expect(getByText('testColumn2')).toBeInTheDocument();
        expect(getByText('8')).toBeInTheDocument();
        expect(getByText('33.30%')).toBeInTheDocument();
        expect(getByText('9')).toBeInTheDocument();
        expect(getByText('44.40%')).toBeInTheDocument();
        expect(getByText('10')).toBeInTheDocument();
        expect(getByText('11')).toBeInTheDocument();
        expect(getByText('12')).toBeInTheDocument();
        expect(getByText('13')).toBeInTheDocument();
        expect(getByText('14')).toBeInTheDocument();
        expect(getByText('value4')).toBeInTheDocument();
        expect(getByText('value5')).toBeInTheDocument();
        expect(getByText('value6')).toBeInTheDocument();
    });

    it('renders profile without field stats', () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <SnapshotStatsView profile={missingFieldStatsProfile} />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Row Count
        expect(getByText('1000')).toBeInTheDocument();
        expect(getByText('Rows')).toBeInTheDocument();

        // Column Count
        expect(getByText('2000')).toBeInTheDocument();
        expect(getByText('Columns')).toBeInTheDocument();

        // Field Profiles
        // First column
        expect(queryByText('testColumn')).toBeNull();
        expect(queryByText('1')).toBeNull();
        expect(queryByText('11.10%')).toBeNull();
        expect(queryByText('2')).toBeNull();
        expect(queryByText('22.20%')).toBeNull();
        expect(queryByText('3')).toBeNull();
        expect(queryByText('4')).toBeNull();
        expect(queryByText('5')).toBeNull();
        expect(queryByText('6')).toBeNull();
        expect(queryByText('value1')).toBeNull();
        expect(queryByText('value2')).toBeNull();
        expect(queryByText('value3')).toBeNull();

        // Second column
        expect(queryByText('testColumn2')).toBeNull();
        expect(queryByText('8')).toBeNull();
        expect(queryByText('33.30%')).toBeNull();
        expect(queryByText('9')).toBeNull();
        expect(queryByText('44.40%')).toBeNull();
        expect(queryByText('10')).toBeNull();
        expect(queryByText('11')).toBeNull();
        expect(queryByText('12')).toBeNull();
        expect(queryByText('13')).toBeNull();
        expect(queryByText('14')).toBeNull();
        expect(queryByText('value4')).toBeNull();
        expect(queryByText('value5')).toBeNull();
        expect(queryByText('value6')).toBeNull();
    });

    it('renders profile without table stats', () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <SnapshotStatsView profile={missingTableStatsProfile} />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Row Count
        expect(queryByText('1000')).toBeNull();
        expect(queryByText('Rows')).toBeNull();
        expect(queryByText('Row Count Unknown')).toBeInTheDocument();

        // Column Count
        expect(queryByText('2000')).toBeNull();
        expect(queryByText('Columns')).toBeNull();
        expect(queryByText('Column Count Unknown')).toBeInTheDocument();

        // Field Profiles
        // First column
        expect(getByText('testColumn')).toBeInTheDocument();
        expect(getByText('1')).toBeInTheDocument();
        expect(getByText('11.10%')).toBeInTheDocument();
        expect(getByText('2')).toBeInTheDocument();
        expect(getByText('22.20%')).toBeInTheDocument();
        expect(getByText('3')).toBeInTheDocument();
        expect(getByText('4')).toBeInTheDocument();
        expect(getByText('5')).toBeInTheDocument();
        expect(getByText('6')).toBeInTheDocument();
        expect(getByText('value1')).toBeInTheDocument();
        expect(getByText('value2')).toBeInTheDocument();
        expect(getByText('value3')).toBeInTheDocument();

        // Second column
        expect(getByText('testColumn2')).toBeInTheDocument();
        expect(getByText('8')).toBeInTheDocument();
        expect(getByText('33.30%')).toBeInTheDocument();
        expect(getByText('9')).toBeInTheDocument();
        expect(getByText('44.40%')).toBeInTheDocument();
        expect(getByText('10')).toBeInTheDocument();
        expect(getByText('11')).toBeInTheDocument();
        expect(getByText('12')).toBeInTheDocument();
        expect(getByText('13')).toBeInTheDocument();
        expect(getByText('14')).toBeInTheDocument();
        expect(getByText('value4')).toBeInTheDocument();
        expect(getByText('value5')).toBeInTheDocument();
        expect(getByText('value6')).toBeInTheDocument();
    });
});
