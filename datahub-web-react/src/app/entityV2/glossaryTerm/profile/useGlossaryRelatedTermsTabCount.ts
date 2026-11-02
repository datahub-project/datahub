import { useEntityData } from '@app/entity/shared/EntityContext';
import { relatedTermsCountVariables, relatedTermsTotal } from '@app/entityV2/glossaryTerm/profile/relatedTermsQuery';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';

export default function useGlossaryRelatedTermsTabCount() {
    const { urn } = useEntityData();
    // Same first-page variables as the Related Terms tab, so the badge and the list share one request.
    const { data } = useGetRelatedTermsQuery({
        variables: { urn, ...relatedTermsCountVariables({}) },
        skip: !urn,
        fetchPolicy: 'cache-and-network',
    });

    const term = data?.glossaryTerm?.urn === urn ? data.glossaryTerm : undefined;
    if (!term) {
        return undefined;
    }
    return relatedTermsTotal(term);
}
