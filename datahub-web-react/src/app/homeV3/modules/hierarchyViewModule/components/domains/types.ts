import { ListDomainsQuery } from '@graphql/domain.generated';

type DomainsArray = NonNullable<ListDomainsQuery['listDomains']>['domains'];
export type DomainItem = DomainsArray extends Array<infer T> ? T : never;
