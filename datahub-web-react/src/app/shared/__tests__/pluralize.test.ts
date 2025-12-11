/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { pluralize } from '@app/shared/textUtil';

describe('pluralize text based on the count', () => {
    it('pluralize regular word with count greater than 1', () => {
        expect(pluralize(2, 'User')).toEqual('Users');
    });
    it('pluralize regular word with count equal to 1', () => {
        expect(pluralize(1, 'User')).toEqual('User');
    });
    it('pluralize regular word with lower case with count greater than 1', () => {
        expect(pluralize(25, 'column')).toEqual('columns');
    });
    it('pluralize regular word with lower case with count equal to 1', () => {
        expect(pluralize(1, 'row')).toEqual('row');
    });
    it('pluralize regular word with suffix provded as es', () => {
        expect(pluralize(20, 'tax', 'es')).toEqual('taxes');
    });
    it('pluralize regular word with suffix provded as ren', () => {
        expect(pluralize(100, 'child', 'ren')).toEqual('children');
    });
    it('pluralize regular word with suffix provded as ren with count equal to 1', () => {
        expect(pluralize(1, 'child', 'ren')).toEqual('child');
    });
    it('pluralize irregular word present in the list', () => {
        expect(pluralize(5, 'query')).toEqual('queries');
    });
    it('pluralize irregular word present in the list with capital first letter', () => {
        expect(pluralize(50, 'Query')).toEqual('queries');
    });
    it('pluralize irregular word present in the list with count equal to 1', () => {
        expect(pluralize(1, 'query')).toEqual('query');
    });
});
