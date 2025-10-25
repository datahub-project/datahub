import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';
import type { MockedFunction } from 'vitest';

import { useMentionInput } from '@app/chat/hooks/useMentionInput';
import { htmlToMarkdown } from '@app/chat/utils/markdownUtils';
import { getActiveMention } from '@app/chat/utils/mentionUtils';

import type { Entity } from '@types';
import { EntityType } from '@types';

// Mock the markdown utilities
vi.mock('@app/chat/utils/markdownUtils', () => ({
    htmlToMarkdown: vi.fn(),
}));

// Mock the mention utilities
vi.mock('@app/chat/utils/mentionUtils', () => ({
    getActiveMention: vi.fn(),
}));

const mockHtmlToMarkdown = htmlToMarkdown as MockedFunction<typeof htmlToMarkdown>;
const mockGetActiveMention = getActiveMention as MockedFunction<typeof getActiveMention>;

describe('useMentionInput', () => {
    const mockOnChange = vi.fn();
    const mockOnEntitySelect = vi.fn();
    const mockElement = document.createElement('div');

    beforeEach(() => {
        vi.clearAllMocks();
        mockHtmlToMarkdown.mockReturnValue('test markdown');
        mockGetActiveMention.mockReturnValue(null);
    });

    it('should initialize with default state', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        expect(result.current.mentionState).toEqual({
            isActive: false,
            query: '',
            startIndex: -1,
            coordinates: { top: 0, left: 0 },
        });
        expect(result.current.contentEditableRef.current).toBeNull();
    });

    it('should handle input without active mention', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Mock element
        (result.current.contentEditableRef as any).current = mockElement;
        mockGetActiveMention.mockReturnValue(null);

        act(() => {
            result.current.handleInput();
        });

        // New behavior: onChange IS called during typing to keep parent in sync
        expect(mockOnChange).toHaveBeenCalledWith('test markdown');
        expect(result.current.mentionState.isActive).toBe(false);
    });

    it.skip('should handle input with active mention', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Mock element
        (result.current.contentEditableRef as any).current = mockElement;
        mockGetActiveMention.mockReturnValue({
            startIndex: 3,
            query: 'user',
        });

        act(() => {
            result.current.handleInput();
        });

        // New behavior: onChange is NOT called during typing
        expect(mockOnChange).not.toHaveBeenCalled();
        expect(result.current.mentionState.isActive).toBe(true);
        expect(result.current.mentionState.query).toBe('user');
        expect(result.current.mentionState.startIndex).toBe(3);
    });

    it.skip('should handle entity selection', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        const mockEntity: Entity = {
            urn: 'urn:li:corpuser:user123',
            type: EntityType.CorpUser,
        };

        // Set up mention state
        act(() => {
            result.current.setMentionState({
                isActive: true,
                query: 'user',
                startIndex: 3,
                coordinates: { top: 100, left: 200 },
            });
        });

        // Mock element; simulate range by setting innerHTML with an @ and placing selection
        (result.current.contentEditableRef as any).current = mockElement;
        mockElement.innerHTML = 'Hello @use';
        const textNode = mockElement.firstChild as Text;
        const sel = window.getSelection();
        const r = document.createRange();
        r.setStart(textNode, mockElement.textContent!.length);
        r.collapse(true);
        sel?.removeAllRanges();
        sel?.addRange(r);

        act(() => {
            result.current.handleEntitySelect(mockEntity, 'John Doe');
        });

        expect(mockOnChange).toHaveBeenCalled();
        expect(mockOnEntitySelect).toHaveBeenCalledWith(mockEntity, 'John Doe');
        expect(result.current.mentionState.isActive).toBe(false);
    });

    it('should handle keyboard events with active mention', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Set up mention state
        act(() => {
            result.current.setMentionState({
                isActive: true,
                query: 'user',
                startIndex: 3,
                coordinates: { top: 100, left: 200 },
            });
        });

        const mockEvent = {
            key: 'Escape',
            preventDefault: vi.fn(),
        } as unknown as React.KeyboardEvent<HTMLDivElement>;

        act(() => {
            result.current.handleKeyDown(mockEvent);
        });

        expect(mockEvent.preventDefault).toHaveBeenCalled();
        expect(result.current.mentionState.isActive).toBe(false);
    });

    it('should handle keyboard events with Enter key and active mention', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Set up mention state
        act(() => {
            result.current.setMentionState({
                isActive: true,
                query: 'user',
                startIndex: 3,
                coordinates: { top: 100, left: 200 },
            });
        });

        const mockEvent = {
            key: 'Enter',
            shiftKey: false,
            preventDefault: vi.fn(),
        } as unknown as React.KeyboardEvent<HTMLDivElement>;

        act(() => {
            result.current.handleKeyDown(mockEvent);
        });

        expect(mockEvent.preventDefault).toHaveBeenCalled();
    });

    it('should handle keyboard events with Enter key and no active mention', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        const mockEvent = {
            key: 'Enter',
            shiftKey: false,
            preventDefault: vi.fn(),
        } as unknown as React.KeyboardEvent<HTMLDivElement>;

        act(() => {
            result.current.handleKeyDown(mockEvent);
        });

        expect(mockEvent.preventDefault).not.toHaveBeenCalled();
    });

    it('should handle keyboard events with Shift+Enter', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        const mockEvent = {
            key: 'Enter',
            shiftKey: true,
            preventDefault: vi.fn(),
        } as unknown as React.KeyboardEvent<HTMLDivElement>;

        act(() => {
            result.current.handleKeyDown(mockEvent);
        });

        expect(mockEvent.preventDefault).not.toHaveBeenCalled();
    });

    it('should handle deactivating mention state when no active mention found', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Set up mention state
        act(() => {
            result.current.setMentionState({
                isActive: true,
                query: 'user',
                startIndex: 3,
                coordinates: { top: 100, left: 200 },
            });
        });

        // Mock element
        (result.current.contentEditableRef as any).current = mockElement;
        mockGetActiveMention.mockReturnValue(null);

        act(() => {
            result.current.handleInput();
        });

        expect(result.current.mentionState.isActive).toBe(false);
    });

    it('should handle entity selection with no element', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        const mockEntity: Entity = {
            urn: 'urn:li:corpuser:user123',
            type: EntityType.CorpUser,
        };

        // Set up mention state
        act(() => {
            result.current.setMentionState({
                isActive: true,
                query: 'user',
                startIndex: 3,
                coordinates: { top: 100, left: 200 },
            });
        });

        // Don't set the element
        (result.current.contentEditableRef as any).current = null;

        act(() => {
            result.current.handleEntitySelect(mockEntity, 'John Doe');
        });

        expect(mockOnChange).not.toHaveBeenCalled();
    });

    it('should handle input with no element', () => {
        const { result } = renderHook(() =>
            useMentionInput({
                value: 'test',
                onChange: mockOnChange,
                onEntitySelect: mockOnEntitySelect,
            }),
        );

        // Don't set the element
        (result.current.contentEditableRef as any).current = null;

        act(() => {
            result.current.handleInput();
        });

        expect(mockOnChange).not.toHaveBeenCalled();
    });
});
