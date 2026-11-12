import { describe, expect, it } from 'vitest';

import {
    queriesEntityKey,
    selectQueriesListData,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/selectQueriesListData';

const current = { id: 'current' };
const previous = { id: 'previous' };
const datasetA = queriesEntityKey('urn:li:dataset:a');
const datasetB = queriesEntityKey('urn:li:dataset:b', 'urn:li:dataset:sibling');

describe('selectQueriesListData', () => {
    it('uses the current payload when the request has landed', () => {
        expect(
            selectQueriesListData({
                data: current,
                previousData: previous,
                error: undefined,
                entityKey: datasetB,
                loadedEntityKey: datasetA,
            }),
        ).toBe(current);
    });

    it('keeps the previous payload while the same dataset is paging', () => {
        expect(
            selectQueriesListData({
                data: undefined,
                previousData: previous,
                error: undefined,
                entityKey: datasetA,
                loadedEntityKey: datasetA,
            }),
        ).toBe(previous);
    });

    it('drops the previous payload when the dataset changes', () => {
        expect(
            selectQueriesListData({
                data: undefined,
                previousData: previous,
                error: undefined,
                entityKey: datasetB,
                loadedEntityKey: datasetA,
            }),
        ).toBeUndefined();
    });

    it('drops the previous payload when the request fails', () => {
        expect(
            selectQueriesListData({
                data: undefined,
                previousData: previous,
                error: new Error('failed'),
                entityKey: datasetA,
                loadedEntityKey: datasetA,
            }),
        ).toBeUndefined();
    });
});

describe('queriesEntityKey', () => {
    it('treats a sibling change as a different dataset', () => {
        expect(queriesEntityKey('urn:li:dataset:a')).not.toBe(queriesEntityKey('urn:li:dataset:a', 'urn:li:dataset:b'));
    });
});
