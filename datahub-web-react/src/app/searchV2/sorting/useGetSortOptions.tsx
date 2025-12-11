/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SORT_OPTIONS } from '@app/searchV2/context/constants';

export default function useGetSortOptions() {
    // TODO: Add a new endpoint showSortFields() that passes the list of potential sort fields, and verifies
    // whether there are any entries matching that sort field.
    return SORT_OPTIONS;
}
