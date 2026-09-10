import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';

const mockPush = vi.fn();
let mockPathname = '/';

vi.mock('react-router-dom', () => ({
    useHistory: () => ({ push: mockPush }),
    useLocation: () => ({ pathname: mockPathname }),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: (_type: unknown, urn: string) => `/document/${urn}`,
    }),
}));

describe('useDocumentNavigation', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockPathname = '/';
    });

    describe('getCurrentDocumentUrn', () => {
        it('decodes the document urn from the current route', () => {
            mockPathname = `/document/${encodeURIComponent('urn:li:document:abc')}`;
            const { result } = renderHook(() => useDocumentNavigation());
            expect(result.current.getCurrentDocumentUrn()).toBe('urn:li:document:abc');
        });

        it('returns null when the route is not a document page', () => {
            mockPathname = '/search';
            const { result } = renderHook(() => useDocumentNavigation());
            expect(result.current.getCurrentDocumentUrn()).toBe(null);
        });
    });

    describe('handleDocumentClick', () => {
        it('navigates to the entity url when not in selection mode', () => {
            const { result } = renderHook(() => useDocumentNavigation());
            result.current.handleDocumentClick('urn:li:document:x');
            expect(mockPush).toHaveBeenCalledWith('/document/urn:li:document:x');
        });

        it('selects rather than navigates when onSelectDocument is provided', () => {
            const onSelectDocument = vi.fn();
            const { result } = renderHook(() => useDocumentNavigation(onSelectDocument));
            result.current.handleDocumentClick('urn:li:document:y');
            expect(onSelectDocument).toHaveBeenCalledWith('urn:li:document:y');
            expect(mockPush).not.toHaveBeenCalled();
        });
    });
});
