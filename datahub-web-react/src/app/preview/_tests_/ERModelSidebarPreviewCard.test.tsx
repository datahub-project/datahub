import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { PreviewType } from '@app/entity/Entity';
import ERModelSidebarPreviewCard from '@app/preview/ERModelSidebarPreviewCard';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import {
    Domain,
    EntityType,
    ErModelRelationshipCardinality,
    GlobalTags,
    GlossaryTermAssociation,
    GlossaryTerms,
    Owner,
    OwnerType,
    TagAssociation,
} from '@types';

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

const mockOwners: Owner[] = [
    {
        owner: {
            urn: 'urn:li:corpuser:owner1',
            type: EntityType.CorpUser,
        } as OwnerType,
        associatedUrn: 'urn:li:corpuser:owner1',
    },
];

const mockCardinality: ErModelRelationshipCardinality = ErModelRelationshipCardinality.OneN;
const mockDescription =
    'This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes.';

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

    it('renders owners in the right column when provided', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard name="Test Entity" urn="test-urn" url="/test-url" owners={mockOwners} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Owners')).toBeInTheDocument();
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

    it('renders cardinality when provided', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard
                        name="Test Entity"
                        urn="test-urn"
                        url="/test-url"
                        cardinality={mockCardinality}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Cardinality:')).toBeInTheDocument();
        expect(screen.getByText(ErModelRelationshipCardinality.OneN)).toBeInTheDocument();
    });

    it('renders nothing when no tags, glossary terms, domain, or cardinality are provided', () => {
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
        expect(screen.queryByText('Cardinality:')).not.toBeInTheDocument();
    });

    it('renders description and toggles "Show More" and "Show Less"', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <ERModelSidebarPreviewCard
                        name="Test Entity"
                        urn="test-urn"
                        url="/test-url"
                        description={mockDescription}
                        previewType={PreviewType.HOVER_CARD}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Verify the description is rendered
        expect(
            screen.getByText(
                'This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purposes. This is a sample description for testing purpo...',
            ),
        ).toBeInTheDocument();

        // Verify "Show More" link is rendered
        const showMoreLink = screen.getByText('Show More');
        expect(showMoreLink).toBeInTheDocument();

        // Click "Show More" and verify "Show Less" link appears
        fireEvent.click(showMoreLink);
        const showLessLink = screen.getByText('Show Less');
        expect(showLessLink).toBeInTheDocument();

        // Click "Show Less" and verify "Show More" link appears again
        fireEvent.click(showLessLink);
        expect(screen.getByText('Show More')).toBeInTheDocument();
    });
});
