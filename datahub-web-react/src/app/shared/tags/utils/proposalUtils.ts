import { ActionRequest } from '../../../../types.generated';

export function findTopLevelProposals(proposals: ActionRequest[]) {
    return proposals.filter((proposal) => !proposal.subResource);
}

export function findFieldPathProposal(proposals: ActionRequest[], fieldPath: string) {
    return proposals.filter((proposal) => proposal.subResource === fieldPath);
}
