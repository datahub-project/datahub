import React from 'react';
import { render } from '@testing-library/react';
import { Properties } from '../../../shared/components/legacy/Properties';
import { sampleProperties } from '../stories/properties';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';

describe('Properties', () => {
    it('renders', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Properties properties={sampleProperties} />,
            </TestPageContainer>,
        );
        expect(getByText('Properties')).toBeInTheDocument();
        expect(getByText('Number of Partitions')).toBeInTheDocument();
        expect(getByText('18')).toBeInTheDocument();
        expect(getByText('Cluster Name')).toBeInTheDocument();
        expect(getByText('Testing')).toBeInTheDocument();
    });
});
