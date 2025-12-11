/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ListDomainsQuery } from '@graphql/domain.generated';

type DomainsArray = NonNullable<ListDomainsQuery['listDomains']>['domains'];
export type DomainItem = DomainsArray extends Array<infer T> ? T : never;
