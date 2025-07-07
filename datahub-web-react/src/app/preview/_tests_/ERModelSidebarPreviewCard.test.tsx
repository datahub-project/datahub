import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import ERModelSidebarPreviewCard from '@app/preview/ERModelSidebarPreviewCard';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { Domain, EntityType, GlobalTags, GlossaryTermAssociation, GlossaryTerms, TagAssociation } from '@types';

const mockTags: GlobalTags = {
    tags: [{ tag: { name: 'Tag1' } }, { tag: { name: 'Tag2' } }] as TagAssociation[],
};

const mockGlossaryTerms: GlossaryTerms = {
    terms: [{ term: { name: 'GlossaryTerm1' } }, { term: { name: 'GlossaryTerm2' } }] as GlossaryTermAssociation[],
};

const mockDomain: Domain = {
    urn: 'urn:li:domain:domain',
    id: 'domain',
    type: EntityType.Domain,
};

describe('ERModelSidebarPreviewCard', () => {
    it('renders tags, glossary terms, and domain', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard
                        name="Test Entity"
                        urn="test-urn"
                        url="/test-url"
                        tags={mockTags}
                        glossaryTerms={mockGlossaryTerms}
                        domain={mockDomain}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Tag1')).toBeInTheDocument();
        expect(screen.getByText('Tag2')).toBeInTheDocument();
        expect(screen.getByText('GlossaryTerm1')).toBeInTheDocument();
        expect(screen.getByText('GlossaryTerm2')).toBeInTheDocument();
        expect(screen.getByText('domain')).toBeInTheDocument();
    });

    it('renders only tags when glossary terms and domain are absent', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard name="Test Entity" urn="test-urn" url="/test-url" tags={mockTags} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Tag1')).toBeInTheDocument();
        expect(screen.getByText('Tag2')).toBeInTheDocument();
        expect(screen.queryByText('GlossaryTerm1')).not.toBeInTheDocument();
        expect(screen.queryByText('domain')).not.toBeInTheDocument();
    });

    it('renders only glossary terms when tags and domain are absent', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard
                        name="Test Entity"
                        urn="test-urn"
                        url="/test-url"
                        glossaryTerms={mockGlossaryTerms}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('GlossaryTerm1')).toBeInTheDocument();
        expect(screen.getByText('GlossaryTerm2')).toBeInTheDocument();
        expect(screen.queryByText('Tag1')).not.toBeInTheDocument();
        expect(screen.queryByText('domain')).not.toBeInTheDocument();
    });

    it('renders nothing when no tags, glossary terms, or domain are provided', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard name="Test Entity" urn="test-urn" url="/test-url" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByText('Tag1')).not.toBeInTheDocument();
        expect(screen.queryByText('GlossaryTerm1')).not.toBeInTheDocument();
        expect(screen.queryByText('domain')).not.toBeInTheDocument();
    });
});
