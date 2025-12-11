/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export default function useGetSemanticVersionFromUrlParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const semanticVersion: string = params.semantic_version as string;
    return semanticVersion;
}
