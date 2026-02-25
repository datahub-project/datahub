import { ListDomainsQuery } from '@graphql/domain.generated';
import { RootGlossaryNodeWithFourLayersFragment } from '@graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '@types';

type DomainsArray = NonNullable<ListDomainsQuery['listDomains']>['domains'];
export type DomainItem = DomainsArray extends Array<infer T> ? T : never;

export type GlossaryNodeType = GlossaryNode | RootGlossaryNodeWithFourLayersFragment;
export type GlossaryTermType = GlossaryTerm | ChildGlossaryTermFragment;
export type GlossaryItemType = GlossaryNodeType | GlossaryTermType;
