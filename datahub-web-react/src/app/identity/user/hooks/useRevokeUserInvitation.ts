import { gql, useMutation } from '@apollo/client';

export const REVOKE_USER_INVITATION_MUTATION = gql`
    mutation revokeUserInvitation($userUrn: String!) {
        revokeUserInvitation(userUrn: $userUrn)
    }
`;

export interface RevokeUserInvitationMutationVariables {
    userUrn: string;
}

export interface RevokeUserInvitationMutationResponse {
    revokeUserInvitation: boolean;
}

export const useRevokeUserInvitationMutation = () => {
    return useMutation<RevokeUserInvitationMutationResponse, RevokeUserInvitationMutationVariables>(
        REVOKE_USER_INVITATION_MUTATION,
    );
};
