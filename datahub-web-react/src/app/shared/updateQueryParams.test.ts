/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Location } from 'history';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import updateQueryParams from '@app/shared/updateQueryParams';

type MockHistory = {
    replace: ReturnType<typeof vi.fn>;
};

function createLocation(pathname: string, search: string): Location {
    return { pathname, search, state: null, key: 'test' } as unknown as Location;
}

describe('updateQueryParams', () => {
    let history: MockHistory;

    beforeEach(() => {
        history = { replace: vi.fn() };
    });

    const getReplaceArgs = () => (history.replace as any).mock.calls[0][0];

    it('preserves plus-encoded values (3%2B) from existing params', () => {
        const location = createLocation('/path', '?q=3%2B');

        updateQueryParams({}, location, history as any);

        expect(getReplaceArgs()).toEqual({
            pathname: '/path',
            search: 'q=3%2B',
        });
    });

    it('does not convert plus-encoded (3%2B) into space-encoded (3%20) when merging', () => {
        const location = createLocation('/search', '?q=3%2B');

        updateQueryParams({ page: '1' }, location, history as any);

        const args = getReplaceArgs();
        expect(args.pathname).toBe('/search');
        expect(args.search).toContain('q=3%2B');
        expect(args.search).not.toContain('%20');
        expect(args.search).toContain('page=1');
    });
});
