import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { ActionRequestAssignee } from '@src/types.generated';

export const PERSONAL_ACTION_REQUESTS_GROUP_NAME = 'Inbox';
export const MY_PROPOSALS_GROUP_NAME = 'My Proposals';

export type ActionRequestGroup = {
    name: string;
    displayName: string;
    assignee: ActionRequestAssignee;
};

export const entityHasProposals = (entityData: GenericEntityProperties | null) => {
    if (!entityData) {
        return false;
    }

    // Todo: This will eventually be changed to entityData.proposals?.length
    return !!entityData.termProposals?.length || !!entityData.tagProposals?.length;
};
