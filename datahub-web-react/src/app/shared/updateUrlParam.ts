/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { RouteComponentProps } from 'react-router';

export function updateUrlParam(history: RouteComponentProps['history'], key: string, value: string) {
    const url = new URL(window.location.href);
    const { searchParams } = url;
    searchParams.set(key, value);
    history.replace(url.search);
}
