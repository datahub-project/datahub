import {
    ROOT_NODES,
    ROOT_TERMS,
    deriveGlossaryLabelFromUrn,
    getGlossaryRootToUpdate,
    getParentNodeToUpdate,
    updateGlossarySidebar,
} from '@app/glossaryV2/utils';
import { glossaryNode1, glossaryNode3, glossaryTerm1 } from '@src/Mocks';

import { EntityType } from '@types';

const glossaryTermWithParent = {
    ...glossaryTerm1,
    parentNodes: {
        count: 1,
        nodes: [glossaryNode1],
    },
};

describe('glossary utils tests', () => {
    it('should get the direct parent node urn in getParentNodeToUpdate for glossary nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryNode3 as any, EntityType.GlossaryNode);
        expect(parentNode).toBe(glossaryNode3.parentNodes?.nodes[0].urn);
    });

    it('should get the direct parent node urn in getParentNodeToUpdate for glossary terms', () => {
        const parentNode = getParentNodeToUpdate(glossaryTermWithParent as any, EntityType.GlossaryTerm);
        expect(parentNode).toBe(glossaryNode1.urn);
    });

    it('should return ROOT_NODES for glossary node with no parent nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryNode1 as any, EntityType.GlossaryNode);
        expect(parentNode).toBe(ROOT_NODES);
    });

    it('should return ROOT_TERMS for glossary term with no parent nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryTerm1 as any, EntityType.GlossaryTerm);
        expect(parentNode).toBe(ROOT_TERMS);
    });

    it('should return ROOT_NODES for glossary node', () => {
        expect(getGlossaryRootToUpdate(EntityType.GlossaryNode)).toBe(ROOT_NODES);
    });

    it('should return ROOT_TERMS for glossary term', () => {
        expect(getGlossaryRootToUpdate(EntityType.GlossaryTerm)).toBe(ROOT_TERMS);
    });

    it('should return updateGlossarySidebar for glossary', () => {
        const parentNodesToUpdate = ['sampleParentNode1', 'sampleParentNode2'];
        const urnsToUpdate = ['urnsSample1'];
        const setUrnsToUpdate = vi.fn();
        updateGlossarySidebar(parentNodesToUpdate, urnsToUpdate, setUrnsToUpdate);
        expect(setUrnsToUpdate).toHaveBeenCalledWith([...urnsToUpdate, ...parentNodesToUpdate]);
    });

    describe('deriveGlossaryLabelFromUrn', () => {
        it('returns the leaf segment after the last dot for nested terms', () => {
            // Glossary URNs encode hierarchy after the type prefix; only the leaf is the user-
            // facing name (the rest comes from the entity's parentNodes when hydrated).
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryTerm:Adoption.HighRisk')).toBe('HighRisk');
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryNode:Classification.Personal.Identifiable')).toBe(
                'Identifiable',
            );
        });

        it('returns the full id when the URN has no dot-encoded hierarchy', () => {
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryTerm:Confidential')).toBe('Confidential');
        });

        it('falls back to the full input when it is not a colon-prefixed URN', () => {
            // Defensive: in practice callers always pass a real URN, but the helper should never
            // throw on a malformed string — it's a last-resort label fallback.
            expect(deriveGlossaryLabelFromUrn('Confidential')).toBe('Confidential');
        });
    });
});
