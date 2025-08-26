/**
 * A preview of a Slack Message
 */
export type SlackMessagePreview = {
    text: string;
    authorName: string;
    authorImageUrl?: string;
    channelName: string;
    timestamp: number;
    workspaceName?: string;
    replyCount?: number;
    url?: string;
};
