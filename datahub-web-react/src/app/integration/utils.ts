import { SlackMessagePreview } from './types';

export const decodeSlackMessagePreview = (json: string): SlackMessagePreview | null => {
    const parsedJson = JSON.parse(json);
    // Validation: If the payload has all required fields, then retur the previe object.
    if (
        parsedJson.text &&
        parsedJson.authorName &&
        parsedJson.channelName &&
        parsedJson.authorImageUrl &&
        parsedJson.timestamp
    ) {
        return parsedJson as SlackMessagePreview;
    }
    // Else return null (cannot render preview)
    return null;
};
