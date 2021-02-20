import { useGetUserQuery } from '../graphql/user.generated';

/**
 * Fetch an instance of EntityRegistry from the React context.
 */
export function useGetAuthenticatedUser() {
    return useGetUserQuery({ variables: { urn: localStorage.getItem('userUrn') as string } });
}
