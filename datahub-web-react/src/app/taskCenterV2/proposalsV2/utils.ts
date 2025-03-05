import { ActionRequestAssignee } from '@src/types.generated';

export const PERSONAL_ACTION_REQUESTS_GROUP_NAME = 'Inbox';
export const MY_PROPOSALS_GROUP_NAME = 'My Proposals';

export type ActionRequestGroup = {
    name: string;
    displayName: string;
    assignee: ActionRequestAssignee;
};
