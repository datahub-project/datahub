import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import TagTermGroup from '../TagTermGroup';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { EntityType, GlobalTags, GlossaryTerms } from '../../../../types.generated';
import { mocks } from '../../../../Mocks';

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
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup editableTags={globalTags1 as GlobalTags} canRemove />
                </TestPageContainer>
            </MockedProvider>,
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
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup uneditableTags={globalTags2 as GlobalTags} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Add Tag')).not.toBeInTheDocument();
        expect(getByText('NeedsOwnership')).toBeInTheDocument();
        expect(queryByLabelText('close')).not.toBeInTheDocument();
    });

    it('renders both together', () => {
        const { getByText, queryByText, queryAllByLabelText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup
                        uneditableTags={globalTags1 as GlobalTags}
                        editableTags={globalTags2 as GlobalTags}
                        canRemove
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Add Tag')).not.toBeInTheDocument();
        expect(getByText('Legacy')).toBeInTheDocument();
        expect(getByText('NeedsOwnership')).toBeInTheDocument();
        expect(queryAllByLabelText('close')).toHaveLength(1);
    });

    it('renders create tag', () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup
                        entityUrn="urn:li:chart:123"
                        entityType={EntityType.Chart}
                        uneditableTags={globalTags1 as GlobalTags}
                        editableTags={globalTags2 as GlobalTags}
                        canRemove
                        canAddTag
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Add Tags')).toBeInTheDocument();
        expect(queryByText('Search for tag...')).not.toBeInTheDocument();
        const AddTagButton = getByText('Add Tags');
        fireEvent.click(AddTagButton);
        expect(queryByText('Search for tag...')).toBeInTheDocument();
    });

    it('renders create term', () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup
                        entityUrn="urn:li:chart:123"
                        entityType={EntityType.Chart}
                        uneditableTags={globalTags1 as GlobalTags}
                        editableTags={globalTags2 as GlobalTags}
                        canRemove
                        canAddTerm
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Add Terms')).toBeInTheDocument();
        expect(queryByText('Search for glossary term...')).not.toBeInTheDocument();
        const AddTagButton = getByText('Add Terms');
        fireEvent.click(AddTagButton);
        expect(queryByText('Search for glossary term...')).toBeInTheDocument();
    });

    it('renders terms', () => {
        const { getByText, queryAllByLabelText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <TagTermGroup
                        entityUrn="urn:li:chart:123"
                        entityType={EntityType.Chart}
                        uneditableGlossaryTerms={glossaryTerms as GlossaryTerms}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('InstrumentIdentifier')).toBeInTheDocument();
        expect(getByText('InstrumentCost')).toBeInTheDocument();
        expect(queryAllByLabelText('book').length).toBe(2);
    });
});
