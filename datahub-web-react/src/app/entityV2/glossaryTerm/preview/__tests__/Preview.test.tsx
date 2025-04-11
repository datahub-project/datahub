import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { mocks } from '../../../../../Mocks';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { Preview } from '../Preview';
import { PreviewType } from '../../../Entity';

describe('Preview', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <Preview
                        data={null}
                        urn="urn:li:glossaryTerm:instruments.FinancialInstrument_v1"
                        name="custom_name"
                        description="definition"
                        owners={null}
                        previewType={PreviewType.PREVIEW}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('custom_name')).toBeInTheDocument();
    });
});
