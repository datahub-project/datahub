import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useGetLastViewedAnnouncementTime } from '@app/homeV2/shared/useGetLastViewedAnnouncementTime';
import { useGetAnnouncementsForUser } from '@app/homeV3/announcements/useGetAnnouncementsForUser';

import { useListPostsQuery } from '@graphql/post.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { PostContentType, PostType } from '@types';

vi.mock('@app/context/useUserContext');
vi.mock('@app/homeV2/shared/useGetLastViewedAnnouncementTime');
vi.mock('@graphql/post.generated');
vi.mock('@graphql/user.generated');

const mockUser = {
    settings: {
        homePage: {
            dismissedAnnouncementUrns: ['urn:1'],
        },
    },
};

const mockPosts = [
    {
        urn: 'urn:2',
        postType: PostType.HomePageAnnouncement,
        content: { contentType: PostContentType.Text },
    },
    {
        urn: 'urn:3',
        postType: PostType.HomePageAnnouncement,
        content: { contentType: PostContentType.Text },
    },
    {
        urn: 'urn:4',
        postType: PostType.EntityAnnouncement,
        content: { contentType: PostContentType.Text },
    },
    {
        urn: 'urn:5',
        postType: PostType.HomePageAnnouncement,
        content: { contentType: PostContentType.Link },
    },
];

describe('useGetAnnouncementsForUser', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return filtered announcements and loading state', () => {
        (useUserContext as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            user: {
                settings: {
                    homePage: {
                        dismissedAnnouncementUrns: ['urn:1', '', null, undefined, 'urn:2'],
                    },
                },
            },
        });
        (useGetLastViewedAnnouncementTime as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ time: 123 });
        (useListPostsQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: {
                listPosts: {
                    posts: mockPosts,
                },
            },
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        (useUpdateUserHomePageSettingsMutation as unknown as ReturnType<typeof vi.fn>).mockReturnValue([vi.fn()]);

        const { result } = renderHook(() => useGetAnnouncementsForUser());

        const callArgs = (useListPostsQuery as any).mock.calls[0][0];
        const urnFilter = callArgs.variables.input.orFilters[0].and.find((f: any) => f.field === 'urn');
        expect(urnFilter.values).toEqual(['urn:1', 'urn:2']);

        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBeNull();
        // Only posts with type HomePageAnnouncement and contentType Text
        expect(result.current.announcements.map((p) => p.urn)).toEqual(['urn:2', 'urn:3']);
    });

    it('should call updateUserHomePageSettings on dismiss', async () => {
        const updateUserHomePageSettings = vi.fn().mockResolvedValue({});
        const refetch = vi.fn();

        (useUserContext as any).mockReturnValue({ user: mockUser });
        (useGetLastViewedAnnouncementTime as any).mockReturnValue({ time: 123 });
        (useListPostsQuery as any).mockReturnValue({
            data: { listPosts: { posts: mockPosts } },
            loading: false,
            error: null,
            refetch,
        });
        (useUpdateUserHomePageSettingsMutation as any).mockReturnValue([updateUserHomePageSettings]);

        const { result } = renderHook(() => useGetAnnouncementsForUser());

        await act(async () => {
            result.current.onDismissAnnouncement('urn:2');
            await new Promise((resolve) => {
                setTimeout(resolve, 2100);
            });
        });

        expect(updateUserHomePageSettings).toHaveBeenCalledWith({
            variables: { input: { newDismissedAnnouncements: ['urn:2'] } },
        });
    });

    it('should filter out announcements that are in newDismissedUrns', async () => {
        (useUserContext as any).mockReturnValue({
            user: {
                settings: {
                    homePage: {
                        dismissedAnnouncementUrns: [],
                    },
                },
            },
        });
        (useGetLastViewedAnnouncementTime as any).mockReturnValue({ time: 123 });
        (useListPostsQuery as any).mockReturnValue({
            data: { listPosts: { posts: mockPosts } },
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        (useUpdateUserHomePageSettingsMutation as any).mockReturnValue([vi.fn()]);

        const { result } = renderHook(() => useGetAnnouncementsForUser());

        expect(result.current.announcements.map((p) => p.urn)).toEqual(['urn:2', 'urn:3']);

        await act(async () => {
            result.current.onDismissAnnouncement('urn:2');
            await new Promise((resolve) => {
                setTimeout(resolve, 10);
            });
        });

        expect(result.current.announcements.map((p) => p.urn)).toEqual(['urn:3']);
    });

    it('should handle when lastViewedAnnouncementsTime is undefined', () => {
        (useUserContext as any).mockReturnValue({ user: mockUser });
        (useGetLastViewedAnnouncementTime as any).mockReturnValue({});
        const useListPostsQueryMock = useListPostsQuery as unknown as ReturnType<typeof vi.fn>;
        useListPostsQueryMock.mockReturnValue({
            data: { listPosts: { posts: mockPosts } },
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        (useUpdateUserHomePageSettingsMutation as any).mockReturnValue([vi.fn()]);

        renderHook(() => useGetAnnouncementsForUser());

        const callArgs = useListPostsQueryMock.mock.calls[0][0];
        const lastModifiedFilter = callArgs.variables.input.orFilters[0].and.find(
            (f: any) => f.field === 'lastModified',
        );
        expect(lastModifiedFilter.values).toEqual(['0']);
    });

    it('should skip query if user is not present', () => {
        (useUserContext as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ user: null });
        (useGetLastViewedAnnouncementTime as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ time: 123 });
        const useListPostsQueryMock = useListPostsQuery as unknown as ReturnType<typeof vi.fn>;
        useListPostsQueryMock.mockReturnValue({
            data: null,
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        (useUpdateUserHomePageSettingsMutation as unknown as ReturnType<typeof vi.fn>).mockReturnValue([vi.fn()]);

        renderHook(() => useGetAnnouncementsForUser());
        expect(useListPostsQueryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
    });

    it('should skip query if lastViewedTimeLoading is true', () => {
        (useUserContext as any).mockReturnValue({ user: mockUser });
        (useGetLastViewedAnnouncementTime as any).mockReturnValue({ time: null, loading: true });
        const useListPostsQueryMock = useListPostsQuery as unknown as ReturnType<typeof vi.fn>;
        useListPostsQueryMock.mockReturnValue({
            data: null,
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        (useUpdateUserHomePageSettingsMutation as any).mockReturnValue([vi.fn()]);

        renderHook(() => useGetAnnouncementsForUser());
        expect(useListPostsQueryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
    });
});
