/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { Tab } from '@components/components/Tabs/Tabs';

import { AnnouncementsTab } from '@app/homeV2/content/tabs/announcements/AnnouncementsTab';
import { DiscoveryTab } from '@app/homeV2/content/tabs/discovery/DiscoveryTab';
import {
    V2_HOME_PAGE_ANNOUNCEMENTS_ID,
    V2_HOME_PAGE_DISCOVER_ID,
} from '@app/onboarding/configV2/HomePageOnboardingConfig';

export enum TabType {
    Discover = 'Discover',
    Activity = 'Activity',
    Announcements = 'Announcements',
}

export const DISCOVER_TAB: Tab = {
    key: TabType.Discover,
    name: 'Discover',
    tooltip: 'Explore your data', // icon: CompassOutlined,
    component: <DiscoveryTab />,
    id: V2_HOME_PAGE_DISCOVER_ID,
};

export const ANNOUNCEMENTS_TAB: Tab = {
    key: TabType.Announcements,
    name: 'Announcements',
    tooltip: 'Announcements from your organization', // icon: NotificationOutlined,
    component: <AnnouncementsTab />,
    id: V2_HOME_PAGE_ANNOUNCEMENTS_ID,
};

export const DEFAULT_TAB = TabType.Discover;
