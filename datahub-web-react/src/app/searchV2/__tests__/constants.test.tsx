/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FIELD_TO_LABEL, ORDERED_FIELDS } from '@app/searchV2/utils/constants';

describe('constants', () => {
    it('ensure that all ordered fields have a corresponding label', () => {
        expect(ORDERED_FIELDS.filter((field) => !Object.keys(FIELD_TO_LABEL).includes(field)).length).toEqual(0);
    });
});
