import { DiscoveryTab } from './discovery/DiscoveryTab';
import { ActivityTab } from './activity/ActivityTab';
import { AnnouncementsTab } from './announcements/AnnouncementsTab';

// todo: decide whether we want icons for each tab!

export enum TabType {
    Discover = 'Discover',
    Activity = 'Activity',
    Announcements = 'Announcements',
}

export const tabs = [
    {
        type: TabType.Discover,
        name: 'Discover',
        description: 'Explore your data',
        // icon: CompassOutlined,
        component: DiscoveryTab,
    },
    {
        type: TabType.Activity,
        name: 'Activity',
        description: 'Recent activity for you',
        // icon: RocketOutlined,
        component: ActivityTab,
    },
    {
        type: TabType.Announcements,
        name: 'Announcements',
        description: 'Announcements from your organization',
        // icon: NotificationOutlined,
        component: AnnouncementsTab,
    },
];

export const TAB_NAME_DETAILS = new Map();
tabs.forEach((tab) => TAB_NAME_DETAILS.set(tab.type, tab));

export const DEFAULT_TAB = TabType.Discover;
