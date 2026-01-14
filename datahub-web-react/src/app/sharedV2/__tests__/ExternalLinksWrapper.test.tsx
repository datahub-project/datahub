import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { ExternalLinksWrapper } from '@app/sharedV2/ExternalLinksWrapper';

describe('ExternalLinksWrapper', () => {
    it('renders children correctly', () => {
        render(
            <ExternalLinksWrapper>
                <span>Test content</span>
            </ExternalLinksWrapper>,
        );

        expect(screen.getByText('Test content')).toBeInTheDocument();
    });

    it('applies className when provided', () => {
        render(
            <ExternalLinksWrapper className="test-class">
                <span>Test content</span>
            </ExternalLinksWrapper>,
        );

        const container = screen.getByTestId('external-links-wrapper');
        expect(container).toHaveClass('test-class');
    });

    it('does not apply className when not provided', () => {
        render(
            <ExternalLinksWrapper>
                <span>Test content</span>
            </ExternalLinksWrapper>,
        );

        const container = screen.getByTestId('external-links-wrapper');
        expect(container).not.toHaveClass('test-class');
    });

    it('sets target="_blank" for external links', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example.com">External Link</a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('target', '_blank');
    });

    it('does not override target="_blank" if already set', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example.com" target="_blank" rel="noreferrer">
                    External Link
                </a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('target', '_blank');
    });

    it('does not set target="_blank" for internal links', () => {
        render(
            <ExternalLinksWrapper>
                <a href="/internal-page">Internal Link</a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('target', '_blank');
    });

    it('adds rel="noopener noreferrer" to external links', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example.com">External Link</a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('preserves existing rel attributes and adds noopener noreferrer', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example.com" rel="nofollow">
                    External Link
                </a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('rel', 'nofollow noopener noreferrer');
    });

    it('does not duplicate noopener noreferrer if already present', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example.com" rel="noopener noreferrer">
                    External Link
                </a>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('handles multiple links correctly', () => {
        render(
            <ExternalLinksWrapper>
                <a href="https://example1.com">Link 1</a>
                <a href="https://example2.com">Link 2</a>
                <span>Non-link element</span>
            </ExternalLinksWrapper>,
        );

        const links = screen.getAllByRole('link');
        expect(links).toHaveLength(2);

        links.forEach((link) => {
            expect(link).toHaveAttribute('target', '_blank');
            expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        });
    });

    it('does not affect non-anchor elements', () => {
        render(
            <ExternalLinksWrapper>
                <span>Span element</span>
                <button type="button">Button element</button>
                <div>Div element</div>
            </ExternalLinksWrapper>,
        );

        expect(screen.getByText('Span element')).toBeInTheDocument();
        expect(screen.getByText('Button element')).toBeInTheDocument();
        expect(screen.getByText('Div element')).toBeInTheDocument();
    });

    it('works with nested elements containing links', () => {
        render(
            <ExternalLinksWrapper>
                <div>
                    <p>Some text</p>
                    <a href="https://nested-link.com">Nested Link</a>
                    <p>More text</p>
                </div>
            </ExternalLinksWrapper>,
        );

        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('href', 'https://nested-link.com');
        expect(link).toHaveAttribute('target', '_blank');
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
});
