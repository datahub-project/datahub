import { entityCapabilities, globalTags, owners } from '../../../Mocks';
import { EntityCapabilityType } from '../../entityV2/Entity';
import { entityHasCapability, getHighlightedTag, getUniqueOwners } from '../utils';

describe('Preview V2 utils tests', () => {
    it('getUniqueOwners -> should return all unique owners based on URN', () => {
        const result = getUniqueOwners(owners);
        const expectedResult = [owners[0], owners[2]];
        expect(result).toMatchObject(expectedResult);
    });
    it('getUniqueOwners -> should return null when no owners are passed', () => {
        const result = getUniqueOwners(null);
        expect(result).toStrictEqual(undefined);
    });
    it('entityHasCapability -> check if entity has the provided capability', () => {
        const capabilityToCheck: EntityCapabilityType = EntityCapabilityType.OWNERS;
        const result = entityHasCapability(entityCapabilities, capabilityToCheck);
        expect(result).toBe(true);
    });
    it('entityHasCapability -> check if entity does not have the provided capability', () => {
        const capabilityToCheck = 'DELEGATE';
        const result = entityHasCapability(entityCapabilities, capabilityToCheck as any);
        expect(result).toBe(false);
    });
    it('getHighlightedTag -> get name of the tag', () => {
        const result = getHighlightedTag(globalTags);
        expect(result).toMatch('abc-sample-tag');
    });
    it('getHighlightedTag -> get empty output for empty tag', () => {
        const result = getHighlightedTag();
        expect(result).toMatch('');
    });
});
