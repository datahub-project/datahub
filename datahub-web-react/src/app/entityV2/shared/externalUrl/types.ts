import { OverflowListItem } from '@src/app/sharedV2/OverflowList';

export interface LinkAttributes {
    url: string;
    label: string;
    onClick?: () => void;
    className?: string;
}

export interface LinkItem extends OverflowListItem {
    url: string;
    description: string;
    attributes: LinkAttributes;
}
