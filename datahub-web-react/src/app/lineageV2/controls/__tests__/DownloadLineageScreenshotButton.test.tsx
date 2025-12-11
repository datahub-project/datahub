/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe('Entity name cleaning', () => {
    it('should clean special characters', () => {
        const cleanName = (name: string) => name.replace(/[^a-zA-Z0-9_-]/g, '_');

        expect(cleanName('dataset-with/special@chars#and$symbols')).toBe('dataset-with_special_chars_and_symbols');
        expect(cleanName('user.transactions')).toBe('user_transactions');
        expect(cleanName('normal_name')).toBe('normal_name');
        expect(cleanName('123-valid_name')).toBe('123-valid_name');
    });
});
