import { renderHook } from '@testing-library/react-hooks';

import useEditableSchemaFieldInfoMaps from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';

import { EditableSchemaMetadata } from '@types';

describe('useEditableSchemaFieldInfoMaps', () => {
    describe('null / undefined / empty input', () => {
        it('returns empty maps for null metadata', () => {
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(null));
            expect(result.current.exactMap.size).toBe(0);
            expect(result.current.v2NormalizedMap.size).toBe(0);
        });

        it('returns empty maps for undefined metadata', () => {
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(undefined));
            expect(result.current.exactMap.size).toBe(0);
            expect(result.current.v2NormalizedMap.size).toBe(0);
        });

        it('returns empty maps for empty editableSchemaFieldInfo array', () => {
            const metadata: EditableSchemaMetadata = { editableSchemaFieldInfo: [] };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            expect(result.current.exactMap.size).toBe(0);
            expect(result.current.v2NormalizedMap.size).toBe(0);
        });
    });

    describe('exactMap — O(1) lookup by fieldPath', () => {
        it('maps each field path to its info entry', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    { fieldPath: 'fieldA', description: 'A' },
                    { fieldPath: 'fieldB', description: 'B' },
                ],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            expect(result.current.exactMap.get('fieldA')?.description).toBe('A');
            expect(result.current.exactMap.get('fieldB')?.description).toBe('B');
        });

        it('returns undefined for a path not in the map', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [{ fieldPath: 'fieldA' }],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            expect(result.current.exactMap.get('missing')).toBeUndefined();
        });

        it('first occurrence wins when duplicate field paths exist', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    { fieldPath: 'fieldA', description: 'first' },
                    { fieldPath: 'fieldA', description: 'second' },
                ],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            expect(result.current.exactMap.get('fieldA')?.description).toBe('first');
            expect(result.current.exactMap.size).toBe(1);
        });
    });

    describe('v2NormalizedMap — lookup by downgraded path', () => {
        it('groups v2 paths under their downgraded key', () => {
            const v2Path = '[version=2.0].[type=record].testField';
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [{ fieldPath: v2Path, description: 'v2 entry' }],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            const entries = result.current.v2NormalizedMap.get('testfield');
            expect(entries).toHaveLength(1);
            expect(entries![0].description).toBe('v2 entry');
        });

        it('groups a v1 path and its v2 equivalent under the same normalized key', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    { fieldPath: 'testField', description: 'v1' },
                    { fieldPath: '[version=2.0].[type=record].testField', description: 'v2' },
                ],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            const entries = result.current.v2NormalizedMap.get('testfield');
            expect(entries).toHaveLength(2);
        });

        it('preserves all entries (including duplicates) in the v2NormalizedMap array', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    { fieldPath: 'fieldA', description: 'first' },
                    { fieldPath: 'fieldA', description: 'second' },
                ],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            const entries = result.current.v2NormalizedMap.get('fielda');
            expect(entries).toHaveLength(2);
        });

        it('keeps distinct paths under separate normalized keys', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [{ fieldPath: 'fieldA' }, { fieldPath: 'fieldB' }],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            expect(result.current.v2NormalizedMap.get('fielda')).toHaveLength(1);
            expect(result.current.v2NormalizedMap.get('fieldb')).toHaveLength(1);
        });

        it('matches camelCase and lowercased paths under the same key (ING-2174)', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    {
                        fieldPath: 'payload.additionalInfo.rawCounterpartyId',
                        description: 'profiler casing',
                    },
                ],
            };
            const { result } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            const key = 'payload.additionalinfo.rawcounterpartyid';
            const entries = result.current.v2NormalizedMap.get(key);
            expect(entries).toHaveLength(1);
            expect(entries![0].description).toBe('profiler casing');
        });
    });

    describe('memoization', () => {
        it('returns the same maps on re-render when metadata reference is unchanged', () => {
            const metadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [{ fieldPath: 'fieldA' }],
            };
            const { result, rerender } = renderHook(() => useEditableSchemaFieldInfoMaps(metadata));
            const first = result.current;
            rerender();
            expect(result.current).toBe(first);
        });

        it('returns new maps when metadata reference changes', () => {
            const { result, rerender } = renderHook(({ meta }) => useEditableSchemaFieldInfoMaps(meta), {
                initialProps: {
                    meta: { editableSchemaFieldInfo: [{ fieldPath: 'fieldA' }] } as EditableSchemaMetadata,
                },
            });
            const first = result.current;
            rerender({ meta: { editableSchemaFieldInfo: [{ fieldPath: 'fieldB' }] } });
            expect(result.current).not.toBe(first);
            expect(result.current.exactMap.has('fieldB')).toBe(true);
        });
    });
});
