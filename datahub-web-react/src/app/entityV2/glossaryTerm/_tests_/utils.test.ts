/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { getRelatedAssetsUrl, getRelatedEntitiesUrl, sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';

import { EntityType } from '@types';

describe('sortGlossaryTerms', () => {
    it('should correctly sort glossary terms when both nodes are provided', () => {
        const nodeA = {
            type: EntityType.GlossaryTerm,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryTermProperties',
                name: 'test child 2',
            },
        };
        const nodeB = {
            type: EntityType.GlossaryTerm,
            urn: 'urn:li:456',
            properties: {
                __typename: 'GlossaryTermProperties',
                name: 'test child 1',
            },
        };
        const result = sortGlossaryTerms(globalEntityRegistryV2, nodeA, nodeB);
        expect(result).toBeGreaterThan(0);
    });

    it('should not sort glossary terms when both nodes are provided in sorted order', () => {
        const nodeA = {
            type: EntityType.GlossaryTerm,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryTermProperties',
                name: 'test child 1',
            },
        };
        const nodeB = {
            type: EntityType.GlossaryTerm,
            urn: 'urn:li:456',
            properties: {
                __typename: 'GlossaryTermProperties',
                name: 'test child 2',
            },
        };
        const result = sortGlossaryTerms(globalEntityRegistryV2, nodeA, nodeB);
        expect(result).toBeLessThan(0);
    });

    it('should correctly sort glossary terms when only one node is provided', () => {
        const nodeA = {
            type: EntityType.GlossaryTerm,
            urn: 'urn:123',
            properties: {
                __typename: 'GlossaryTermProperties',
                name: 'test child 1',
            },
        };
        const result = sortGlossaryTerms(globalEntityRegistryV2, nodeA);
        expect(result).toBeGreaterThan(0);
    });

    it('should handle null nodes by considering them equal in sorting', () => {
        const result = sortGlossaryTerms(globalEntityRegistryV2);
        expect(result).toBe(0);
    });
});

describe('getRelatedEntitiesUrl', () => {
    it('should return Related Entities URL', () => {
        const urn = 'urn123';
        const url = getRelatedEntitiesUrl(globalEntityRegistryV2, urn);
        const expectedURL = `/glossaryTerm/${urn}/${encodeURIComponent('Related Entities')}`;
        expect(url).toEqual(expectedURL);
    });
});

describe('getRelatedAssetsUrl', () => {
    it('should return Related Assets URL', () => {
        const urn = 'urn123';
        const url = getRelatedAssetsUrl(globalEntityRegistryV2, urn);
        const expectedURL = `/glossaryTerm/${urn}/${encodeURIComponent('Related Assets')}`;
        expect(url).toEqual(expectedURL);
    });
});
