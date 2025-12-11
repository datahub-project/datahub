/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { KEY_SEPARATOR, getReloadableKey, getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';

describe('utils', () => {
    describe('getReloadableKey', () => {
        it('should return the correct key when both keyType and entryId are provided', () => {
            const key = getReloadableKey('testType', 'testId');
            expect(key).toBe(`testType${KEY_SEPARATOR}testId`);
        });

        it('should return the correct key when only keyType is provided', () => {
            const key = getReloadableKey('testType');
            expect(key).toBe(`testType${KEY_SEPARATOR}`);
        });

        it('should handle an empty string for entryId', () => {
            const key = getReloadableKey('testType', '');
            expect(key).toBe(`testType${KEY_SEPARATOR}`);
        });
    });

    describe('getReloadableKeyType', () => {
        it('should return the correct key type', () => {
            const keyType = getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, 'testName');
            expect(keyType).toBe('MODULE>testName');
        });
    });
});
