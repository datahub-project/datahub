/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';

import { EntityType } from '@types';

describe('sortGlossaryNodes', () => {
    it('should correctly sort glossary nodes when both nodes are provided', () => {
        const nodeA = {
            type: EntityType.GlossaryNode,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryNodeProperties',
                name: 'test child 2',
            },
        };
        const nodeB = {
            type: EntityType.GlossaryNode,
            urn: 'urn:li:456',
            properties: {
                __typename: 'GlossaryNodeProperties',
                name: 'test child 1',
            },
        };
        const result = sortGlossaryNodes(globalEntityRegistryV2, nodeA, nodeB);
        expect(result).toBeGreaterThan(0);
    });

    it('should not sort glossary nodes when both nodes are provided in sorted order', () => {
        const nodeA = {
            type: EntityType.GlossaryNode,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryNodeProperties',
                name: 'test child 1',
            },
        };
        const nodeB = {
            type: EntityType.GlossaryNode,
            urn: 'urn:li:456',
            properties: {
                __typename: 'GlossaryNodeProperties',
                name: 'test child 2',
            },
        };
        const result = sortGlossaryNodes(globalEntityRegistryV2, nodeA, nodeB);
        expect(result).toBeLessThan(0);
    });

    it('should correctly sort glossary nodes when only one node is provided', () => {
        const nodeA = {
            type: EntityType.GlossaryNode,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryNodeProperties',
                name: 'test child 1',
            },
        };
        const result = sortGlossaryNodes(globalEntityRegistryV2, nodeA);
        expect(result).toBeGreaterThan(0);
    });

    it('should handle null nodes by considering them equal in sorting', () => {
        const result = sortGlossaryNodes(globalEntityRegistryV2);
        expect(result).toBe(0);
    });
});
