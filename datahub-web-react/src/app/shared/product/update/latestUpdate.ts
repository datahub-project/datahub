import SampleImage from '@images/slack-and-teams.png';

export type ProductUpdate = {
    enabled: boolean;
    id: string;
    title: string;
    image?: string;
    description?: string;
    ctaText: string;
    ctaLink: string;
};

// NOTE: This is a place that OSS and Cloud diverge.
/* Important: Change this section to adjust the system announcement shown in the bottom left corner of the product! */
// TODO: Migrate this to be served via an aspect!
export const latestUpdate: ProductUpdate = {
    enabled: true,
    id: 'v0.3.14', // Very important, when changed it will trigger the announcement to be re-displayed for a user.
    title: 'Ask DataHub in Teams',
    description: 'Ask data questions & get notified in Microsoft Teams',
    image: SampleImage, // Import and use image.,
    ctaText: 'Read about it',
    ctaLink: 'https://datahub.com/blog/datahub-cloud-v0-3-14/',
};
