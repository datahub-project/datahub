import SampleImage from '@images/sample-product-update-image.png';

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
    id: 'v1.2.0', // Very important, when changed it will trigger the announcement to be re-displayed for a user.
    title: "What's New In DataHub",
    description: 'Explore version v1.2.0',
    image: SampleImage, // Import and use image.,
    ctaText: 'Read updates',
    ctaLink: 'https://docs.datahub.com/docs/releases#v1-2-0',
};
