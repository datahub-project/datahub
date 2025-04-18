import { LinkPreviewType } from '../../types.generated';

const SLACK_LINK_PATTERN = '.*.slack.com/archives/.*';

const LINK_PREVIEW_TYPES = [
    {
        type: LinkPreviewType.SlackMessage,
        pattern: SLACK_LINK_PATTERN,
    },
];

/**
 * Determines whether we should try to render a Preview of the Link.
 */
export function shouldTryLinkPreview(link: string) {
    return LINK_PREVIEW_TYPES.some((previewInfo) => {
        const regex = new RegExp(previewInfo.pattern);
        if (regex.test(link)) {
            return true;
        }
        return false;
    });
}
