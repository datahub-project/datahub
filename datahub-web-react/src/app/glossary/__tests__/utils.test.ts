import { EntityType } from '../../../types.generated';
import { glossaryNode1, glossaryNode3, glossaryTerm1 } from '../../../Mocks';
import { getParentNodeToUpdate, getGlossaryRootToUpdate, ROOT_NODES, ROOT_TERMS } from '../utils';

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
        expect(parentNode).toBe(glossaryNode3.parentNodes?.nodes[0]?.urn);
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
});
