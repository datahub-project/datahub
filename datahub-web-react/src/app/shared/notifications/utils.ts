import { ExtraContextForErrorMessage, TestNotificationConfig } from './types';

export const getDestinationId = (
    config: Pick<TestNotificationConfig, 'destinationSettings' | 'integration'>,
): string => {
    let destination = 'no destination specified';
    if (config.integration === 'slack') {
        const { channels, userHandle } = config.destinationSettings;
        if (channels?.length) {
            destination = channels.join(', ');
        } else if (userHandle) {
            destination = userHandle;
        }
    }

    return destination;
};

/**
 * Gets error content to display, guiding user to best course of action to correct the issue
 * NOTE: uses mapping provided by slack docs here: https://api.slack.com/methods/chat.postMessage#errors
 * NOTE: currently assumes current user is not an admin
 * TODO: include isCurrentUserDataHubAdmin in {@param extraContext}, so we can personalize the message to admins vs non-admins
 * @param errorCode based on the codes provided here: https://api.slack.com/methods/chat.postMessage#errors
 * @param extraContext to personalize the message to the user's current situation
 * @returns {string} Error content to display
 */
export const getErrorDisplayContentFromSlackErrorCode = (
    errorCode: string,
    extraContext: ExtraContextForErrorMessage,
): string => {
    let message = `Slack message failed to send with error '${errorCode}'`;
    switch (errorCode) {
        // --- channel/conversation related errors --- //
        case 'channel_not_found':
            message = `Looks like there's no channel with the name '${extraContext.destinationName}' in this workspace. Verify the channel name is spelled correctly; and if this is a private channel, be sure to add the DataHub Slackbot to it.`;
            break;
        case 'not_in_channel':
            message = `Please add the DataHub Slackbot into ${extraContext.destinationName} to recieve messages there.`;
            break;
        case 'is_archived':
            message = `Looks like this channel has been archived. Contact your Slack admin to un-archive it, or select another channel.`;
            break;
        case 'restricted_action_read_only_channel':
            message = `Looks like this channel is read-only. Contact your Slack admin to change this, or select another channel.`;
            break;
        case 'restricted_action_thread_only_channel':
            message = `Looks like this channel only supports replying in threads. Contact your Slack admin to change this, or select another channel.`;
            break;
        case 'no_permission':
            message = `We do not have access to post messages into ${extraContext.destinationName}. Please add the DataHub Slackbot into ${extraContext.destinationName} to recieve messages there.`;
            break;

        // --- Admin restriction errors --- //
        case 'ekm_access_denied':
            message = `Your workspace admins have suspended the ability to post messages. Contact your Slack admin to resolve this, or try again later.`;
            break;
        case 'restricted_action':
            message = `A workspace preference has restricted the DataHub Slackbot from posting messages. Contact your Slack admin to resolve this, or try again later.`;
            break;
        case 'access_denied':
            message = `Access has been denied to complete the request. Contact your DataHub Admin to resolve this, or try again later.`;
            break;

        // --- Team-related errors --- //
        case 'team_access_not_granted':
            // TOOD: show who the admins are that they can contact
            // TODO: link to docs to give necessary access
            message = `The Slackbot has not been given access to the team associated with the desired destination. Contact your DataHub Admin to update the Slack token to one that has access to this team.`;
            break;
        case 'team_not_found':
            // TODO: link to docs to get channel_id
            message = `Could not find the team associated with the destination ${extraContext.destinationName}. If you're using the channel_name, try providing the channel_id instead.`;
            break;

        // --- Token-related errors --- //
        case 'account_inactive':
            message =
                'The DataHub Slackbot token is for a deleted workspace. Contact your DataHub admin to re-connect DataHub to Slack with the latest workspace details.';
            break;
        case 'invalid_auth':
            message = `Either the Slack integration tokens are invalid, or DataHub's IPs have been restricted from sending messages. Contact your DataHub admin to re-connect DataHub to Slack.`;
            break;
        case 'missing_scope':
            message = `The DataHub Slackbot does not have necessary scope to send this message. Contact your DataHub admin to updated the scoped privileges of the Slackbot.`;
            break;
        case 'not_allowed_token_type':
            // Should never happen
            // TODO: include a contact us action
            message = `The DataHub Slackbot's tokentype is not allowed to perform this request. This is unexpected, please contact us if this does not resolve on its own shortly.`;
            break;
        case 'not_authed':
            message = `No authentication token was provided to Slack when performing this request. This is unexpected, please contact us if this does not resolve on its own shortly.`;
            break;
        case 'token_expired':
            message =
                'The DataHub Slackbot token is expired. Contact your DataHub admin to re-connect DataHub to Slack with a fresh token.';
            break;
        case 'token_revoked':
            message =
                "The DataHub Slackbot's token access has been revoked. Contact your DataHub admin to re-connect DataHub to Slack with a fresh token.";
            break;
        case 'accesslimited':
            message =
                'Access to sending messages has been limited on the current network. This may resolve on its own shortly. If it does not resolve itself, you can contact your DataHub admin to troubleshoot this issue.';
            break;

        // --- Error with slack systems --- //
        case 'fatal_error':
            message = `Slack's system experienced a fatal error while completing the request. It's possible the message still sent. This should resolve on its own shortly. If not, please contact your Slack admin to resolve the issue.`;
            break;
        case 'internal_error':
            message = `Slack's system experienced some internal errors while completing this request. This should resolve on its own shortly. If not, please contact your Slack admin to resolve the issue.`;
            break;
        case 'service_unavailable':
            message = `Slack is temporarily unavailable. This should resolve on its own. Please try again once Slack is back up.`;
            break;
        case 'team_added_to_org':
            message = `The workspace associated with your request is currently undergoing migration to an Enterprise Organization. Web API and other platform operations will be intermittently unavailable until the transition is complete.`;
            break;
        case 'org_login_required':
            message = `The workspace is undergoing an enterprise migration and will not be available until migration is complete.`;
            break;

        // --- Limit errors --- //
        case 'ratelimited':
            message = 'Slack has rate limited the request. Please try again after a little while.';
            break;
        case 'rate_limited':
            message = 'Slack has rate limited the request. Please try again after a little while.';
            break;
        case 'message_limit_exceeded':
            message =
                'It seems that there are too many messages being sent within your Slack workspace at the moment. Please try again after a little while.';
            break;
        default:
            break;
    }

    return message;
};
