import { useListActionRequestsQuery } from '../../../../graphql/actionRequest.generated';
import { ActionRequestStatus, AssigneeType } from '../../../../types.generated';
import { useGetAuthenticatedUser } from '../../../useGetAuthenticatedUser';

export type DocumentationProposalsSummary = {
    loading: boolean;
    count: number;
};

// TODO: Support fetching action requests for groups as well.
// Ideally this would happen at the backend layer
export const useGetPendingDocumentationProposals = (): DocumentationProposalsSummary => {
    const currentUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;
    const { loading, data } = useListActionRequestsQuery({
        variables: {
            input: {
                start: 0,
                count: 20,
                status: ActionRequestStatus.Pending,
                assignee: {
                    type: AssigneeType.User,
                    urn: currentUserUrn as string,
                },
            },
        },
        fetchPolicy: 'cache-first',
        skip: !currentUserUrn,
    });
    const total = data?.listActionRequests?.total || 0;
    return {
        loading,
        count: total,
    };
};
