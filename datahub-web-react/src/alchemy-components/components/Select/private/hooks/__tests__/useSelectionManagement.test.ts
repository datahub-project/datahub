import { act, renderHook } from '@testing-library/react-hooks';

import { useSelectionManagement } from '@components/components/Select/private/hooks/useSelectionManagement';

describe('useSelectionManagement', () => {
    const defaultProps = {
        initialValues: ['1', '2'],
        values: undefined,
        onUpdate: undefined,
        isMultiselect: true,
        autocommit: true,
    };

    describe('initialization', () => {
        it('should initialize with initialValues', () => {
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1', '2'],
                }),
            );

            expect(result.current.selectedValues).toEqual(['1', '2']);
            expect(result.current.stagedValues).toEqual(['1', '2']);
        });

        it('should initialize with empty array when no initialValues provided', () => {
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                }),
            );

            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.stagedValues).toEqual([]);
        });
    });

    describe('autocommit mode', () => {
        it('should update both selectedValues and stagedValues when autocommit is true', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                    onUpdate: mockOnUpdate,
                    autocommit: true,
                }),
            );

            act(() => {
                result.current.setStagedValues(['3']);
            });

            expect(result.current.selectedValues).toEqual(['3']);
            expect(result.current.stagedValues).toEqual(['3']);
            expect(mockOnUpdate).toHaveBeenCalledWith(['3']);
        });

        it('should update only stagedValues when autocommit is false', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                    onUpdate: mockOnUpdate,
                    autocommit: false,
                }),
            );

            act(() => {
                result.current.setStagedValues(['3']);
            });

            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.stagedValues).toEqual(['3']);
            expect(mockOnUpdate).not.toHaveBeenCalled();
        });

        it('should override autocommit with options.autocommit', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                    onUpdate: mockOnUpdate,
                    autocommit: false,
                }),
            );

            act(() => {
                result.current.setStagedValues(['3'], { autocommit: true });
            });

            expect(result.current.selectedValues).toEqual(['3']);
            expect(result.current.stagedValues).toEqual(['3']);
            expect(mockOnUpdate).toHaveBeenCalledWith(['3']);
        });
    });

    describe('onValueChanged', () => {
        describe('multiselect mode', () => {
            it('should add value when not already selected', () => {
                const { result } = renderHook(() =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1'],
                        isMultiselect: true,
                        autocommit: false,
                    }),
                );

                act(() => {
                    result.current.onValueChanged('2');
                });

                expect(result.current.stagedValues).toEqual(['1', '2']);
            });

            it('should remove value when already selected', () => {
                const { result } = renderHook(() =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1', '2'],
                        isMultiselect: true,
                        autocommit: false,
                    }),
                );

                act(() => {
                    result.current.onValueChanged('1');
                });

                expect(result.current.stagedValues).toEqual(['2']);
            });
        });

        describe('single select mode', () => {
            it('should replace value with new selection', () => {
                const { result } = renderHook(() =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1'],
                        isMultiselect: false,
                        autocommit: false,
                    }),
                );

                act(() => {
                    result.current.onValueChanged('2');
                });

                expect(result.current.stagedValues).toEqual(['2']);
            });

            it('should keep value selected when clicking already selected option (no-op)', () => {
                const { result } = renderHook(() =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1'],
                        isMultiselect: false,
                        autocommit: false,
                    }),
                );

                act(() => {
                    result.current.onValueChanged('1');
                });

                expect(result.current.stagedValues).toEqual(['1']);
            });
        });
    });

    describe('clearSelection', () => {
        it('should clear all selections', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1', '2'],
                    onUpdate: mockOnUpdate,
                    autocommit: true,
                }),
            );

            act(() => {
                result.current.clearSelection();
            });

            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.stagedValues).toEqual([]);
            expect(mockOnUpdate).toHaveBeenCalledWith([]);
        });

        it('should clear without autocommit when options.autocommit is false', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1', '2'],
                    onUpdate: mockOnUpdate,
                    autocommit: false,
                }),
            );

            // First change staged values without autocommit
            act(() => {
                result.current.setStagedValues(['3', '4'], { autocommit: false });
            });

            // Now clear with autocommit false
            act(() => {
                result.current.clearSelection({ autocommit: false });
            });

            // selectedValues should remain unchanged
            expect(result.current.selectedValues).toEqual(['1', '2']);
            expect(result.current.stagedValues).toEqual([]);
            expect(mockOnUpdate).not.toHaveBeenCalled();
        });
    });

    describe('commitSelection', () => {
        it('should commit staged values to selected values', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1'],
                    onUpdate: mockOnUpdate,
                    autocommit: false,
                }),
            );

            // Stage new values
            act(() => {
                result.current.setStagedValues(['1', '2', '3']);
            });

            // Commit
            act(() => {
                result.current.commitSelection();
            });

            expect(result.current.selectedValues).toEqual(['1', '2', '3']);
            expect(result.current.stagedValues).toEqual(['1', '2', '3']);
            expect(mockOnUpdate).toHaveBeenCalledWith(['1', '2', '3']);
        });

        it('should be a no-op when autocommit is enabled', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1'],
                    onUpdate: mockOnUpdate,
                    autocommit: true,
                }),
            );

            // With autocommit, setStagedValues already commits
            act(() => {
                result.current.setStagedValues(['1', '2']);
            });

            expect(result.current.selectedValues).toEqual(['1', '2']);
            expect(mockOnUpdate).toHaveBeenCalledTimes(1);

            // Calling commitSelection should not trigger another update
            act(() => {
                result.current.commitSelection();
            });

            expect(result.current.selectedValues).toEqual(['1', '2']);
            expect(mockOnUpdate).toHaveBeenCalledTimes(1); // Still only 1 call
        });
    });

    describe('resetStagedValues', () => {
        it('should reset staged values to selected values', () => {
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: ['1', '2'],
                    autocommit: false,
                }),
            );

            // Change staged values without autocommit
            act(() => {
                result.current.setStagedValues(['3', '4'], { autocommit: false });
            });

            // Verify staged values changed
            expect(result.current.stagedValues).toEqual(['3', '4']);

            // Reset
            act(() => {
                result.current.resetStagedValues();
            });

            expect(result.current.stagedValues).toEqual(['1', '2']);
        });
    });

    describe('controlled values sync', () => {
        it('should sync selectedValues and stagedValues when values prop changes', () => {
            const { result, rerender } = renderHook(
                ({ values }) =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1'],
                        values,
                    }),
                {
                    initialProps: { values: undefined },
                },
            );

            // Initial state
            expect(result.current.selectedValues).toEqual(['1']);
            expect(result.current.stagedValues).toEqual(['1']);

            // Change values prop
            rerender({ values: ['2', '3'] } as any);

            expect(result.current.selectedValues).toEqual(['2', '3']);
            expect(result.current.stagedValues).toEqual(['2', '3']);
        });

        it('should not sync when values are equal', () => {
            const { result, rerender } = renderHook(
                ({ values }) =>
                    useSelectionManagement({
                        ...defaultProps,
                        initialValues: ['1'],
                        values,
                    }),
                {
                    initialProps: { values: ['1'] },
                },
            );

            // Rerender with same values
            rerender({ values: ['1'] });

            expect(result.current.selectedValues).toEqual(['1']);
            expect(result.current.stagedValues).toEqual(['1']);
        });
    });

    describe('onUpdate callback', () => {
        it('should call onUpdate when selectedValues change', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                    onUpdate: mockOnUpdate,
                    autocommit: true,
                }),
            );

            act(() => {
                result.current.setStagedValues(['1', '2']);
            });

            expect(mockOnUpdate).toHaveBeenCalledWith(['1', '2']);
        });

        it('should not call onUpdate when only stagedValues change (autocommit false)', () => {
            const mockOnUpdate = vi.fn();
            const { result } = renderHook(() =>
                useSelectionManagement({
                    ...defaultProps,
                    initialValues: [],
                    onUpdate: mockOnUpdate,
                    autocommit: false,
                }),
            );

            act(() => {
                result.current.setStagedValues(['1', '2']);
            });

            expect(mockOnUpdate).not.toHaveBeenCalled();
        });
    });
});
