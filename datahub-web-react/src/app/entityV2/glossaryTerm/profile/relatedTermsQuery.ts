import type { ApolloClient } from '@apollo/client';

import { RelatedTermTypes } from '@app/entityV2/glossaryTerm/profile/RelatedTermTypes';

import type { GetRelatedTermsQuery } from '@graphql/glossary.generated';

export const RELATED_TERMS_PAGE_SIZE = 10;

export type RelatedTermKey = keyof typeof RelatedTermTypes;

export const RELATED_TERM_MENU_KEYS = Object.keys(RelatedTermTypes) as RelatedTermKey[];

export type RelatedTermsTerm = NonNullable<GetRelatedTermsQuery['glossaryTerm']>;

export function relatedTermsCountVariables(counts: Partial<Record<RelatedTermKey, number>>) {
    return {
        isRelatedTermsCount: counts.isRelatedTerms ?? RELATED_TERMS_PAGE_SIZE,
        hasRelatedTermsCount: counts.hasRelatedTerms ?? RELATED_TERMS_PAGE_SIZE,
        isAChildrenCount: counts.isAChildren ?? RELATED_TERMS_PAGE_SIZE,
        containedByCount: counts.containedBy ?? RELATED_TERMS_PAGE_SIZE,
    };
}

export function relationshipGroup(term: RelatedTermsTerm | null | undefined, key: RelatedTermKey) {
    return term?.[key] ?? null;
}

export function relatedTermsTotal(term: RelatedTermsTerm | null | undefined): number {
    return RELATED_TERM_MENU_KEYS.reduce((sum, key) => sum + (term?.[key]?.total ?? 0), 0);
}

export function refreshRelatedTerms(client: Pick<ApolloClient<unknown>, 'refetchQueries'>) {
    return client.refetchQueries({ include: ['getRelatedTerms'] }).catch((refreshError: unknown) => {
        console.error('Failed to refresh glossary related terms', refreshError);
    });
}
