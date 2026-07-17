import { describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import {
    calculateDeleteNavigationTarget,
    navigateAfterDelete,
} from '@app/homeV2/layout/sidebar/documents/documentDeleteNavigation';

import { EntityType } from '@types';

// Helper to create test document tree nodes
const createTestNode = (urn: string, parentUrn: string | null = null): DocumentTreeNode => ({
    urn,
    title: `Document ${urn}`,
    parentUrn,
    hasChildren: false,
    children: [],
});

describe('documentDeleteNavigation', () => {
    describe('calculateDeleteNavigationTarget', () => {
        it('should navigate to parent when document has a parent', () => {
            const deletedNode = createTestNode('urn:li:document:child', 'urn:li:document:parent');
            const rootNodes = [createTestNode('urn:li:document:parent')];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:child');

            expect(result).toBe('urn:li:document:parent');
        });

        it('should navigate to next sibling when deleting root document with next sibling', () => {
            const deletedNode = createTestNode('urn:li:document:1', null);
            const rootNodes = [
                createTestNode('urn:li:document:1'),
                createTestNode('urn:li:document:2'),
                createTestNode('urn:li:document:3'),
            ];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:1');

            expect(result).toBe('urn:li:document:2'); // Next sibling
        });

        it('should navigate to previous sibling when deleting last root document', () => {
            const deletedNode = createTestNode('urn:li:document:3', null);
            const rootNodes = [
                createTestNode('urn:li:document:1'),
                createTestNode('urn:li:document:2'),
                createTestNode('urn:li:document:3'),
            ];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:3');

            expect(result).toBe('urn:li:document:2'); // Previous sibling
        });

        it('should navigate to previous sibling when deleting middle root document', () => {
            const deletedNode = createTestNode('urn:li:document:2', null);
            const rootNodes = [
                createTestNode('urn:li:document:1'),
                createTestNode('urn:li:document:2'),
                createTestNode('urn:li:document:3'),
            ];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:2');

            expect(result).toBe('urn:li:document:3'); // Next sibling takes precedence
        });

        it('should return null when deleting only root document', () => {
            const deletedNode = createTestNode('urn:li:document:1', null);
            const rootNodes = [createTestNode('urn:li:document:1')];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:1');

            expect(result).toBe(null); // No siblings
        });

        it('should return null when deletedNode is null', () => {
            const rootNodes = [createTestNode('urn:li:document:1')];

            const result = calculateDeleteNavigationTarget(null, rootNodes, 'urn:li:document:1');

            expect(result).toBe(null);
        });

        it('should return null when deletedNode is undefined', () => {
            const rootNodes = [createTestNode('urn:li:document:1')];

            const result = calculateDeleteNavigationTarget(undefined, rootNodes, 'urn:li:document:1');

            expect(result).toBe(null);
        });

        it('should return null when document not found in rootNodes', () => {
            const deletedNode = createTestNode('urn:li:document:unknown', null);
            const rootNodes = [createTestNode('urn:li:document:1'), createTestNode('urn:li:document:2')];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:unknown');

            expect(result).toBe(null);
        });

        it('should handle empty rootNodes array', () => {
            const deletedNode = createTestNode('urn:li:document:1', null);
            const rootNodes: DocumentTreeNode[] = [];

            const result = calculateDeleteNavigationTarget(deletedNode, rootNodes, 'urn:li:document:1');

            expect(result).toBe(null);
        });
    });

    describe('navigateAfterDelete', () => {
        it('should navigate to document URL when navigationTarget is provided', () => {
            const entityRegistry = {
                getEntityUrl: vi.fn(() => '/document/urn:li:document:target'),
            };
            const history = {
                push: vi.fn(),
            };

            navigateAfterDelete('urn:li:document:target', entityRegistry, history);

            expect(entityRegistry.getEntityUrl).toHaveBeenCalledWith(EntityType.Document, 'urn:li:document:target');
            expect(history.push).toHaveBeenCalledWith('/document/urn:li:document:target');
        });

        it('should navigate to home when navigationTarget is null', () => {
            const entityRegistry = {
                getEntityUrl: vi.fn(),
            };
            const history = {
                push: vi.fn(),
            };

            navigateAfterDelete(null, entityRegistry, history);

            expect(entityRegistry.getEntityUrl).not.toHaveBeenCalled();
            expect(history.push).toHaveBeenCalledWith('/');
        });

        it('should use correct entity type for document URLs', () => {
            const entityRegistry = {
                getEntityUrl: vi.fn((type: EntityType, urn: string) => `/entity/${type}/${urn}`),
            };
            const history = {
                push: vi.fn(),
            };

            navigateAfterDelete('urn:li:document:123', entityRegistry, history);

            expect(entityRegistry.getEntityUrl).toHaveBeenCalledWith(EntityType.Document, 'urn:li:document:123');
        });
    });
});
