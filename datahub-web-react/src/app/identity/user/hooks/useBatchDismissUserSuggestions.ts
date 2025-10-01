import { useMutation } from '@apollo/client';

import { BatchDismissUserSuggestionsDocument } from '@graphql/mutations.generated';

export const useBatchDismissUserSuggestionsMutation = () => {
    return useMutation(BatchDismissUserSuggestionsDocument);
};
