import { ActionRequest, EntityType } from '../../../../types.generated';

export const ENTITY_TYPES_WITH_PROPOSALS = [
    EntityType.Chart,
    EntityType.Container,
    EntityType.Dashboard,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.Dataset,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.MlprimaryKey,
    EntityType.Notebook,
];

export function shouldShowProposeButton(entityType: EntityType) {
    return ENTITY_TYPES_WITH_PROPOSALS.includes(entityType);
}

export function findTopLevelProposals(proposals: ActionRequest[]) {
    return proposals.filter((proposal) => !proposal.subResource);
}

export function findFieldPathProposal(proposals: ActionRequest[], fieldPath: string) {
    return proposals.filter((proposal) => proposal.subResource === fieldPath);
}
