import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, GlobalTags, GlossaryTerms } from '@types';

// Stub the leaf pill components so we can observe exactly what TagTermGroup renders — which urns, in DOM
// order, and whether each is removable — without pulling in the full Tag/Term subtrees (Apollo mutations,
// profile drawers, router links). TagTermGroup's own dedup/ordering logic is what we're exercising.
const isPropagatedAssociation = (association: any) =>
    !!association.attribution?.sourceDetail?.some((d: any) => d.key === 'propagated' && d.value === 'true');

vi.mock('@app/sharedV2/tags/tag/Tag', () => ({
    default: ({ tag, canRemove }: any) => (
        <span
            data-testid="pill"
            data-urn={tag.tag.urn}
            data-removable={String(!!canRemove)}
            data-propagated={String(isPropagatedAssociation(tag))}
        />
    ),
}));
vi.mock('@app/sharedV2/tags/term/Term', () => ({
    default: ({ term, canRemove }: any) => (
        <span
            data-testid="pill"
            data-urn={term.term.urn}
            data-removable={String(!!canRemove)}
            data-propagated={String(isPropagatedAssociation(term))}
        />
    ),
}));
vi.mock('@app/sharedV2/tags/AddTagTerm', () => ({ default: () => null }));

const PROPAGATED = { attribution: { sourceDetail: [{ key: 'propagated', value: 'true' }] } };

const tag = (urn: string, propagated = false) => ({
    tag: { urn, type: EntityType.Tag },
    ...(propagated && PROPAGATED),
});
const term = (urn: string, propagated = false) => ({
    term: { urn, type: EntityType.GlossaryTerm },
    ...(propagated && PROPAGATED),
});

const renderGroup = (props: Record<string, unknown>) =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <TagTermGroup canRemove {...props} />
            </TestPageContainer>
        </MockedProvider>,
    );

const renderedUrns = () => screen.queryAllByTestId('pill').map((el) => el.getAttribute('data-urn'));

describe('TagTermGroup deduplication and ordering', () => {
    it('renders a tag urn only once when it appears in more than one bucket', () => {
        renderGroup({
            uneditableTags: { tags: [tag('urn:li:tag:a', true)] } as GlobalTags,
            editableTags: { tags: [tag('urn:li:tag:a'), tag('urn:li:tag:b')] } as GlobalTags,
        });
        expect(renderedUrns()).toEqual(['urn:li:tag:a', 'urn:li:tag:b']);
    });

    it('renders uneditable tags before editable ones', () => {
        renderGroup({
            editableTags: { tags: [tag('urn:li:tag:editable')] } as GlobalTags,
            uneditableTags: { tags: [tag('urn:li:tag:uneditable')] } as GlobalTags,
        });
        expect(renderedUrns()).toEqual(['urn:li:tag:uneditable', 'urn:li:tag:editable']);
    });

    it('keeps the uneditable (non-removable) copy when the same tag is in both buckets', () => {
        renderGroup({
            uneditableTags: { tags: [tag('urn:li:tag:dup')] } as GlobalTags,
            editableTags: { tags: [tag('urn:li:tag:dup')] } as GlobalTags,
        });
        const pills = screen.getAllByTestId('pill');
        expect(pills).toHaveLength(1);
        // Uneditable wins over the removable duplicate, so no remove action is offered.
        expect(pills[0]).toHaveAttribute('data-removable', 'false');
    });

    it('prefers the user-applied copy over a propagated duplicate of the same tag', () => {
        renderGroup({
            editableTags: { tags: [tag('urn:li:tag:dup', true), tag('urn:li:tag:dup')] } as GlobalTags,
        });
        const pills = screen.getAllByTestId('pill');
        expect(pills).toHaveLength(1);
        expect(pills[0]).toHaveAttribute('data-propagated', 'false');
    });

    it('still displays a propagated tag that has no manual duplicate', () => {
        renderGroup({ editableTags: { tags: [tag('urn:li:tag:propagated', true)] } as GlobalTags });
        const pills = screen.getAllByTestId('pill');
        expect(pills).toHaveLength(1);
        expect(pills[0]).toHaveAttribute('data-urn', 'urn:li:tag:propagated');
        expect(pills[0]).toHaveAttribute('data-propagated', 'true');
    });

    it('dedupes glossary terms across buckets and keeps uneditable-first order', () => {
        renderGroup({
            uneditableGlossaryTerms: { terms: [term('urn:li:glossaryTerm:x', true)] } as GlossaryTerms,
            editableGlossaryTerms: {
                terms: [term('urn:li:glossaryTerm:x'), term('urn:li:glossaryTerm:y')],
            } as GlossaryTerms,
        });
        expect(renderedUrns()).toEqual(['urn:li:glossaryTerm:x', 'urn:li:glossaryTerm:y']);
    });
});

// data-removable reflects the canRemove passed to each pill, which gates whether the remove/delete
// action is rendered. Uneditable tags/terms come from ingestion/another platform and must not be removable.
describe('TagTermGroup remove action', () => {
    it('does not offer a remove action for uneditable tags', () => {
        renderGroup({ uneditableTags: { tags: [tag('urn:li:tag:managed')] } as GlobalTags });
        expect(screen.getByTestId('pill')).toHaveAttribute('data-removable', 'false');
    });

    it('does not offer a remove action for uneditable terms', () => {
        renderGroup({ uneditableGlossaryTerms: { terms: [term('urn:li:glossaryTerm:managed')] } as GlossaryTerms });
        expect(screen.getByTestId('pill')).toHaveAttribute('data-removable', 'false');
    });

    it('offers a remove action for editable tags and terms', () => {
        renderGroup({
            editableTags: { tags: [tag('urn:li:tag:mine')] } as GlobalTags,
            editableGlossaryTerms: { terms: [term('urn:li:glossaryTerm:mine')] } as GlossaryTerms,
        });
        screen.getAllByTestId('pill').forEach((pill) => expect(pill).toHaveAttribute('data-removable', 'true'));
    });
});
