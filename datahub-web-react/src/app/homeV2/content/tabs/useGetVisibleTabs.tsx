import { Tab } from '@components/components/Tabs/Tabs';

import analytics, { EventType, HomePageModule } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ANNOUNCEMENTS_TAB, DISCOVER_TAB, TabType } from '@app/homeV2/content/tabs/tabs';
import { useUpdateLastViewedAnnouncementTime } from '@app/homeV2/shared/updateLastViewedAnnouncementTime';
import { useGetLastViewedAnnouncementTime } from '@app/homeV2/shared/useGetLastViewedAnnouncementTime';
import { hasViewedAnnouncement } from '@app/homeV2/shared/utils';
import { getHomePagePostsFilters } from '@app/utils/queryUtils';

import { useListPostsQuery } from '@graphql/post.generated';
import { PostContentType, PostType } from '@types';

const handleTabClick = (module: HomePageModule) => {
    analytics.event({
        type: EventType.HomePageClick,
        module,
    });
};

const useGetAnnouncementsExists = (): Tab | null => {
    const { user } = useUserContext();
    const { time: lastViewedAnnouncementsTime, refetch } = useGetLastViewedAnnouncementTime();
    const { updateLastViewedAnnouncementTime } = useUpdateLastViewedAnnouncementTime();
    const { data } = useListPostsQuery({
        variables: {
            input: {
                start: 0,
                count: 30,
                orFilters: getHomePagePostsFilters(),
            },
        },
        fetchPolicy: 'cache-first',
    });

    const onSelectTab = () => {
        if (user?.urn) {
            updateLastViewedAnnouncementTime(user?.urn).then(() => {
                refetch();
            });
        }
        handleTabClick(HomePageModule.Announcements);
    };

    const activePosts = data?.listPosts?.posts?.filter(
        (post) => post.postType === PostType.HomePageAnnouncement && post.content.contentType === PostContentType.Text,
    );
    const activePostsCount = activePosts?.length || 0;

    const unseenPosts = activePosts?.filter(
        (post) => !hasViewedAnnouncement(lastViewedAnnouncementsTime, post.lastModified?.time),
    );
    const unseenPostsCount = unseenPosts?.length || 0;

    if (activePostsCount >= 0) {
        return {
            ...ANNOUNCEMENTS_TAB,
            count: unseenPostsCount,
            onSelectTab,
        };
    }
    return null;
};

const useGetActivityExists = (): Tab | null => {
    // TODO: Activity tab
    return null;
};

export type ActiveTab = {
    key: TabType;
    count?: number;
    onSelectTab?: () => void; // Refetch count, etc
};

export const useGetActiveTabs = (): Tab[] => {
    const activeTabs: Tab[] = [{ ...DISCOVER_TAB, onSelectTab: () => handleTabClick(HomePageModule.Discover) }];

    const activityTab = useGetActivityExists();
    const announcementsTab = useGetAnnouncementsExists();

    if (activityTab) {
        activeTabs.push(activityTab);
    }

    if (announcementsTab) {
        activeTabs.push(announcementsTab);
    }

    return activeTabs;
};
