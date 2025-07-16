import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import { useCreateModuleModalState } from '@app/homeV3/context/hooks/useCreateModuleModalState';
import type { ModulePositionInput } from '@app/homeV3/template/types';

import type { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

const mockModule: PageModuleFragment = {
    urn: 'urn:li:pageModule:new',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'New Module',
        type: DataHubPageModuleType.AssetCollection,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
};

const anotherMockModule: PageModuleFragment = {
    urn: 'urn:li:pageModule:another',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Second Module',
        type: DataHubPageModuleType.AssetCollection,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
};

const TYPE = DataHubPageModuleType.AssetCollection;
const TYPE2 = DataHubPageModuleType.Domains;

const POS1: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
const POS2: ModulePositionInput = { rowIndex: 1, rowSide: 'right' };
const POS3: ModulePositionInput = { rowIndex: 5, rowSide: 'left' };

describe('useCreateModuleModalState', () => {
    it('should initialize to default state', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        expect(result.current.isOpen).toBe(false);
        expect(result.current.moduleType).toBeNull();
        expect(result.current.position).toBeNull();
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should set correct state on open()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        expect(result.current.isOpen).toBe(true);
        expect(result.current.moduleType).toBe(TYPE);
        expect(result.current.position).toEqual(POS1);
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should update state to latest args on consecutive open()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        act(() => result.current.open(TYPE2, POS2));
        expect(result.current.moduleType).toBe(TYPE2);
        expect(result.current.position).toEqual(POS2);
        expect(result.current.isOpen).toBe(true);
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should handle different position values on open()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE2, POS3));
        expect(result.current.position).toEqual(POS3);
    });

    it('should set edit state module on openToEdit()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.openToEdit(TYPE, mockModule));
        expect(result.current.isOpen).toBe(true);
        expect(result.current.moduleType).toBe(TYPE);
        expect(result.current.isEditing).toBe(true);
        expect(result.current.initialState).toBe(mockModule);
        expect(result.current.position).toBeNull();
    });

    it('should update to latest args on consecutive openToEdit()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.openToEdit(TYPE, mockModule));
        act(() => result.current.openToEdit(TYPE2, anotherMockModule));
        expect(result.current.isOpen).toBe(true);
        expect(result.current.moduleType).toBe(TYPE2);
        expect(result.current.isEditing).toBe(true);
        expect(result.current.initialState).toBe(anotherMockModule);
        expect(result.current.position).toBeNull();
    });

    it('should reset state after open() then close()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        act(() => result.current.close());
        expect(result.current.isOpen).toBe(false);
        expect(result.current.moduleType).toBeNull();
        expect(result.current.position).toBeNull();
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should reset state after openToEdit() then close()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.openToEdit(TYPE, mockModule));
        act(() => result.current.close());
        expect(result.current.isOpen).toBe(false);
        expect(result.current.moduleType).toBeNull();
        expect(result.current.position).toBeNull();
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should reset edit state on open() after openToEdit()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.openToEdit(TYPE, mockModule));
        act(() => result.current.open(TYPE2, POS2));
        expect(result.current.isOpen).toBe(true);
        expect(result.current.moduleType).toBe(TYPE2);
        expect(result.current.position).toEqual(POS2);
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should maintain correct state on multiple alternations between open and openToEdit', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        expect(result.current.position).toEqual(POS1);
        act(() => result.current.openToEdit(TYPE, mockModule));
        expect(result.current.position).toEqual(POS1);
        act(() => result.current.open(TYPE, POS2));
        expect(result.current.position).toEqual(POS2);
        act(() => result.current.openToEdit(TYPE2, mockModule));
        expect(result.current.position).toEqual(POS2);
    });

    it('should fully reset all state after a complex sequence and close()', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        act(() => result.current.openToEdit(TYPE2, mockModule));
        act(() => result.current.open(TYPE, POS2));
        act(() => result.current.close());
        expect(result.current.isOpen).toBe(false);
        expect(result.current.moduleType).toBeNull();
        expect(result.current.position).toBeNull();
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should not throw and always reset state on multiple close() calls', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        act(() => result.current.close());
        act(() => result.current.close());
        expect(result.current.isOpen).toBe(false);
        expect(result.current.moduleType).toBeNull();
        expect(result.current.position).toBeNull();
        expect(result.current.isEditing).toBe(false);
        expect(result.current.initialState).toBeNull();
    });

    it('should retain latest initialState and position after successive edits', () => {
        const { result } = renderHook(() => useCreateModuleModalState());
        act(() => result.current.open(TYPE, POS1));
        act(() => result.current.openToEdit(TYPE, mockModule));
        act(() => result.current.openToEdit(TYPE, anotherMockModule));
        expect(result.current.initialState).toBe(anotherMockModule);
        expect(result.current.position).toEqual(POS1);
    });
});
