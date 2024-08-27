import { glossaryNode1, glossaryNode2, glossaryNode3, glossaryNode4 } from '../../../../../Mocks';
import { filterResultsForMove } from '../NodeParentSelect';

describe('filterResultsForMove', () => {
    it('should return true if the given node is different than given urn and the node is not a child of the given urn', () => {
        const shouldKeep = filterResultsForMove(glossaryNode4, glossaryNode1.urn);
        expect(shouldKeep).toBe(true);
    });

    it('should return false if the given node has the same urn as the given urn', () => {
        const shouldKeep = filterResultsForMove(glossaryNode1, glossaryNode1.urn);
        expect(shouldKeep).toBe(false);
    });

    it('should return false if the given node is a direct child of the given urn', () => {
        const shouldKeep = filterResultsForMove(glossaryNode2, glossaryNode1.urn);
        expect(shouldKeep).toBe(false);
    });

    it('should return false if the given node is a child of a child of the given urn', () => {
        const shouldKeep = filterResultsForMove(glossaryNode3, glossaryNode1.urn);
        expect(shouldKeep).toBe(false);
    });
});
