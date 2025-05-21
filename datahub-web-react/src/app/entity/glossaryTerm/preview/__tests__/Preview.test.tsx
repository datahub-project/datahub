import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { PreviewType } from '@app/entity/Entity';
import { Preview } from '@app/entity/glossaryTerm/preview/Preview';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('Preview', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <Preview
                        urn="urn:li:glossaryTerm:instruments.FinancialInstrument_v1"
                        name="name"
                        description="definition"
                        owners={null}
                        previewType={PreviewType.PREVIEW}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('definition')).toBeInTheDocument();
    });
});
