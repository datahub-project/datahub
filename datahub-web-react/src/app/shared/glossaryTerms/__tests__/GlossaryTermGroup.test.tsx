import React from 'react';
import { render } from '@testing-library/react';
import GlossaryTermGroup from '../GlossaryTermGroup';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { EntityType, GlossaryTerms } from '../../../../types.generated';

const identifierTerm = {
    urn: 'urn:li:glossaryTerm:intruments.InstrumentIdentifier',
    name: 'InstrumentIdentifier',
    type: EntityType.GlossaryTerm,
};

const costTerm = {
    urn: 'urn:li:glossaryTerm:intruments.InstrumentCost',
    name: 'InstrumentCost',
    type: EntityType.GlossaryTerm,
};

const glossaryTerms = {
    terms: [{ term: identifierTerm }, { term: costTerm }],
};

describe('GlossaryTermGroup', () => {
    it('renders terms', () => {
        const { getByText, queryAllByLabelText } = render(
            <TestPageContainer>
                <GlossaryTermGroup glossaryTerms={glossaryTerms as GlossaryTerms} />
            </TestPageContainer>,
        );
        expect(getByText('InstrumentIdentifier')).toBeInTheDocument();
        expect(getByText('InstrumentCost')).toBeInTheDocument();
        expect(queryAllByLabelText('book').length).toBe(2);
    });
});
