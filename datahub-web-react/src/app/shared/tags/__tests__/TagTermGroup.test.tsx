import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import TagTermGroup from '../TagTermGroup';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { EntityType, GlossaryTerms } from '../../../../types.generated';

const legacyTag = {
    urn: 'urn:li:tag:legacy',
    name: 'Legacy',
    description: 'this element is outdated',
    type: EntityType.Tag,
};

const ownershipTag = {
    urn: 'urn:li:tag:NeedsOwnership',
    name: 'NeedsOwnership',
    description: 'this element needs an owner',
    type: EntityType.Tag,
};

const globalTags1 = {
    tags: [{ tag: legacyTag }],
};

const globalTags2 = {
    tags: [{ tag: ownershipTag }],
};

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

describe('TagTermGroup', () => {
    it('renders editable tags', async () => {
        const { getByText, getByLabelText, queryAllByLabelText, queryByText } = render(
            <TestPageContainer>
                <TagTermGroup editableTags={globalTags1} canRemove />
            </TestPageContainer>,
        );
        expect(queryByText('Add Tag')).not.toBeInTheDocument();
        expect(getByText('Legacy')).toBeInTheDocument();
        expect(queryAllByLabelText('close')).toHaveLength(1);
        fireEvent.click(getByLabelText('close'));
        await waitFor(() => expect(getByText('Do you want to remove Legacy tag?')).toBeInTheDocument());
        expect(getByText('Do you want to remove Legacy tag?')).toBeInTheDocument();

        fireEvent.click(getByLabelText('Close'));

        await waitFor(() => expect(queryByText('Do you want to remove Legacy tag?')).not.toBeInTheDocument());

        expect(getByText('Legacy')).toBeInTheDocument();
    });

    it('renders uneditable tags', () => {
        const { getByText, queryByLabelText, queryByText } = render(
            <TestPageContainer>
                <TagTermGroup uneditableTags={globalTags2} />
            </TestPageContainer>,
        );
        expect(queryByText('Add Tag')).not.toBeInTheDocument();
        expect(getByText('NeedsOwnership')).toBeInTheDocument();
        expect(queryByLabelText('close')).not.toBeInTheDocument();
    });

    it('renders both together', () => {
        const { getByText, queryByText, queryAllByLabelText } = render(
            <TestPageContainer>
                <TagTermGroup uneditableTags={globalTags1} editableTags={globalTags2} canRemove />
            </TestPageContainer>,
        );
        expect(queryByText('Add Tag')).not.toBeInTheDocument();
        expect(getByText('Legacy')).toBeInTheDocument();
        expect(getByText('NeedsOwnership')).toBeInTheDocument();
        expect(queryAllByLabelText('close')).toHaveLength(1);
    });

    it('renders create tag', () => {
        const { getByText, queryByText } = render(
            <TestPageContainer>
                <TagTermGroup uneditableTags={globalTags1} editableTags={globalTags2} canRemove canAdd />
            </TestPageContainer>,
        );
        expect(queryByText('Add Tag')).toBeInTheDocument();
        expect(queryByText('Find a tag')).not.toBeInTheDocument();
        const AddTagButton = getByText('Add Tag');
        fireEvent.click(AddTagButton);
        expect(queryByText('Find a tag')).toBeInTheDocument();
    });

    it('renders terms', () => {
        const { getByText, queryAllByLabelText } = render(
            <TestPageContainer>
                <TagTermGroup glossaryTerms={glossaryTerms as GlossaryTerms} />
            </TestPageContainer>,
        );
        expect(getByText('InstrumentIdentifier')).toBeInTheDocument();
        expect(getByText('InstrumentCost')).toBeInTheDocument();
        expect(queryAllByLabelText('book').length).toBe(2);
    });
});
