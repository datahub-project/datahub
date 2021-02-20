import { useGetUserQuery } from '../graphql/user.generated';

/**
 * Fetch a CorpUser object corresponding to the currently authenticated user.
 */
export function useGetAuthenticatedUser() {
    return useGetUserQuery({ variables: { urn: localStorage.getItem('userUrn') as string } });
}
