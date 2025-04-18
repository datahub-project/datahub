import { getDefaultOwnerEntities, getDefaultOwnershipTypeUrn } from '../utils';
import {
    entityDataWithNoAllowedOwners,
    entityDataWithNoAllowedTypes,
    entityDataWithNoOwners,
    mockEntityData,
    mockEntityData2,
    multipleRestrictedPrompt,
    multipleRestrictedWithTypesPrompt,
    multipleUnrestrictedPrompt,
    multipleUnrestrictedWithSingleTypePrompt,
    multipleUnrestrictedWithTypesPrompt,
    ownershipTypeUrn1,
    ownershipTypeUrn2,
    ownershipTypeUrn3,
    singleRestrictedPrompt,
    singleRestrictedWithTypesPrompt,
    singleUnrestrictedPrompt,
    singleUnrestrictedWithTypesPrompt,
    user1,
    user2,
    user3,
    user4,
} from './mocks';

describe('get default owners based on different conditions', () => {
    test('get default owners with single cardinality and unrestricted owners', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, singleUnrestrictedPrompt);
        expect(defaultOwners).toEqual([user1]);
    });

    test('get default owners with single cardinality and restricted owners', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, singleRestrictedPrompt);
        expect(defaultOwners).toEqual([user2]);
    });

    test('get default owners with multiple cardinality and unrestricted owners', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, multipleUnrestrictedPrompt);
        expect(defaultOwners).toEqual([user1, user2, user3]);
    });

    test('get default owners with multiple cardinality and restricted owners', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, multipleRestrictedPrompt);
        expect(defaultOwners).toEqual([user2, user3]);
    });

    test('get default owners with single cardinality, unrestricted owners and restricted ownership types', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, singleUnrestrictedWithTypesPrompt);
        expect(defaultOwners).toEqual([user1]);
    });

    test('get default owners with single cardinality, restricted owners and restricted ownership types', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, singleRestrictedWithTypesPrompt);
        expect(defaultOwners).toEqual([user2]);
    });

    test('get default owners with multiple cardinality, unrestricted owners and restricted ownership types', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, multipleUnrestrictedWithTypesPrompt);
        expect(defaultOwners).toEqual([user1, user2]);
    });

    test('get default owners with multiple cardinality, restricted owners and restricted ownership types', () => {
        const defaultOwners = getDefaultOwnerEntities(mockEntityData, multipleRestrictedWithTypesPrompt);
        expect(defaultOwners).toEqual([user2]);
    });

    test('get default owners when existing owners are empty', () => {
        const defaultOwners = getDefaultOwnerEntities(entityDataWithNoOwners, multipleUnrestrictedPrompt);
        expect(defaultOwners).toEqual([]);
    });

    test('get default owners when no existing owner matches allowed owners', () => {
        const defaultOwners = getDefaultOwnerEntities(entityDataWithNoAllowedOwners, multipleRestrictedPrompt);
        expect(defaultOwners).toEqual([]);
    });

    test('get default owners when no existing owner is of allowed typess', () => {
        const defaultOwners = getDefaultOwnerEntities(entityDataWithNoAllowedTypes, multipleRestrictedWithTypesPrompt);
        expect(defaultOwners).toEqual([]);
    });
});

describe('get default ownership type urn', () => {
    test('get default ownership type urn with single allowed ownership type', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData, multipleUnrestrictedWithSingleTypePrompt, []);
        expect(defaultOwners).toEqual(ownershipTypeUrn2);
    });

    test('get default ownership type urn with single cardinality and initial value', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData, singleRestrictedWithTypesPrompt, [user1.urn]);
        expect(defaultOwners).toEqual(ownershipTypeUrn1);
    });

    test('get default ownership type urn with single cardinality and empty initial value', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData, singleRestrictedPrompt, []);
        expect(defaultOwners).toEqual(undefined);
    });

    test('get default ownership type urn with empty initial owners and unrestricted types', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData, singleUnrestrictedPrompt, []);
        expect(defaultOwners).toEqual(undefined);
    });

    test('get default ownership type urn with multiple cardinality and multiple allowed ownership types', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData, multipleUnrestrictedWithTypesPrompt, []);
        expect(defaultOwners).toEqual(undefined);
    });

    test('get default ownership type urn when all the selected owners are of one type', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData2, multipleUnrestrictedPrompt, [
            user3.urn,
            user4.urn,
        ]);
        expect(defaultOwners).toEqual(ownershipTypeUrn3);
    });

    test('get default ownership type urn when selected owners are of different types', () => {
        const defaultOwners = getDefaultOwnershipTypeUrn(mockEntityData2, multipleUnrestrictedPrompt, [
            user1.urn,
            user4.urn,
        ]);
        expect(defaultOwners).toEqual(undefined);
    });
});
