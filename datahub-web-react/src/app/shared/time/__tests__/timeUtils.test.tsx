/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { addInterval } from '@app/shared/time/timeUtils';

import { DateInterval } from '@types';

describe('timeUtils', () => {
    describe('addInterval', () => {
        it('add date interval works correctly', () => {
            const input = new Date(1677242304000);
            const afterAdd = addInterval(1, input, DateInterval.Month);
            const expected = new Date(1679661504000);
            expect(afterAdd.getTime()).toEqual(expected.getTime());
        });
    });
});
