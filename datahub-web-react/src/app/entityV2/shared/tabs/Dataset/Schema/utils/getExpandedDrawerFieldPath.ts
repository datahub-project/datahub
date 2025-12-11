/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';

export default function getExpandedDrawerFieldPath(location: any) {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    try {
        return decodeURIComponent(params.highlightedPath ? (params.highlightedPath as string) : '');
    } catch (ex) {
        return '';
    }
}
