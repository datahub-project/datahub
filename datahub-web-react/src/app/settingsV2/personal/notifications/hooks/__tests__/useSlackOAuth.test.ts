import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';

import { useSlackOAuth } from '@app/settingsV2/personal/notifications/hooks/useSlackOAuth';

// Mock dependencies
vi.mock('antd', () => ({
    message: {
        info: vi.fn(),
        error: vi.fn(),
    },
}));

// Mock window.location
const mockLocationHref = vi.fn();
const originalLocation = window.location;

beforeAll(() => {
    delete (window as any).location;
    window.location = {
        ...originalLocation,
        href: '',
        origin: 'http://localhost:9002',
        pathname: '/settings/personal-notifications',
        search: '',
    } as any;
    Object.defineProperty(window.location, 'href', {
        set: mockLocationHref,
        get: () => 'http://localhost:9002/settings/personal-notifications',
    });
});

afterAll(() => {
    Object.defineProperty(window, 'location', { value: originalLocation, writable: true });
});

describe('useSlackOAuth', () => {
    const defaultOptions = {
        isSlackPlatformConfigured: true,
    };

    beforeEach(() => {
        vi.useFakeTimers();
        vi.clearAllMocks();
        // Default: successful connect endpoint response
        global.fetch = vi.fn().mockResolvedValue({
            ok: true,
            json: () =>
                Promise.resolve({
                    authorization_url: 'https://slack.com/openid/connect/authorize?state=nonce123',
                }),
        });
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    describe('startOAuthFlow', () => {
        it('calls connect endpoint and redirects to authorization URL', async () => {
            const { result } = renderHook(() => useSlackOAuth(defaultOptions));

            await act(async () => {
                await result.current.startOAuthFlow();
            });

            expect(global.fetch).toHaveBeenCalledWith('/integrations/slack/connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
            });
            expect(mockLocationHref).toHaveBeenCalledWith('https://slack.com/openid/connect/authorize?state=nonce123');
            expect(message.info).toHaveBeenCalledWith('Redirecting to Slack for authentication...');
        });

        it('shows error when Slack platform is not configured', async () => {
            const { result } = renderHook(() => useSlackOAuth({ ...defaultOptions, isSlackPlatformConfigured: false }));

            await act(async () => {
                await result.current.startOAuthFlow();
            });

            expect(message.error).toHaveBeenCalledWith(
                'Slack platform integration is not properly configured. Please contact your administrator.',
            );
            expect(global.fetch).not.toHaveBeenCalled();
            expect(mockLocationHref).not.toHaveBeenCalled();
        });

        it('shows error when connect endpoint returns no authorization URL', async () => {
            global.fetch = vi.fn().mockResolvedValue({
                ok: true,
                json: () => Promise.resolve({ authorization_url: '' }),
            });

            const { result } = renderHook(() => useSlackOAuth(defaultOptions));

            await act(async () => {
                await result.current.startOAuthFlow();
            });

            expect(message.error).toHaveBeenCalledWith('No authorization URL returned from server');
            expect(mockLocationHref).not.toHaveBeenCalled();
        });

        it('shows error when connect endpoint fails', async () => {
            global.fetch = vi.fn().mockResolvedValue({
                ok: false,
                status: 500,
                json: () => Promise.resolve({ detail: 'Slack app credentials not configured' }),
            });

            const { result } = renderHook(() => useSlackOAuth(defaultOptions));

            await act(async () => {
                await result.current.startOAuthFlow();
            });

            expect(message.error).toHaveBeenCalledWith('Slack app credentials not configured');
            expect(mockLocationHref).not.toHaveBeenCalled();
        });

        it('shows error when fetch throws a network error', async () => {
            global.fetch = vi.fn().mockRejectedValue(new Error('network error'));

            const { result } = renderHook(() => useSlackOAuth(defaultOptions));

            await act(async () => {
                await result.current.startOAuthFlow();
            });

            expect(message.error).toHaveBeenCalledWith('network error');
            expect(mockLocationHref).not.toHaveBeenCalled();
        });
    });

    describe('isLoading', () => {
        it('defaults to false', () => {
            const { result } = renderHook(() => useSlackOAuth(defaultOptions));
            expect(result.current.isLoading).toBe(false);
        });
    });

    describe('handleOAuthRedirect', () => {
        it('redirects on success with slack_user_id', () => {
            // Set URL search params to simulate a successful OAuth callback redirect
            Object.defineProperty(window.location, 'search', {
                value: '?slack_oauth=success&slack_user_id=U12345678',
                configurable: true,
            });

            renderHook(() => useSlackOAuth(defaultOptions));

            // Should redirect to clean pathname (strip query params)
            expect(mockLocationHref).toHaveBeenCalledWith('/settings/personal-notifications');

            // Restore
            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });
        });

        it('shows error message on OAuth error', () => {
            Object.defineProperty(window.location, 'search', {
                value: '?slack_oauth=error&message=access_denied',
                configurable: true,
            });

            renderHook(() => useSlackOAuth(defaultOptions));

            expect(message.error).toHaveBeenCalledWith('Slack connection failed: access_denied');
            expect(mockLocationHref).not.toHaveBeenCalled();

            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });
        });

        it('does nothing when no oauth params in URL', () => {
            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });

            renderHook(() => useSlackOAuth(defaultOptions));

            expect(message.error).not.toHaveBeenCalled();
            expect(mockLocationHref).not.toHaveBeenCalled();
        });
    });

    describe('auto-connect flow', () => {
        it('triggers OAuth flow when ?connect_slack=true is present', async () => {
            Object.defineProperty(window.location, 'search', {
                value: '?connect_slack=true',
                configurable: true,
            });

            // Mock history.replaceState to verify URL cleanup
            const mockReplaceState = vi.fn();
            window.history.replaceState = mockReplaceState;

            renderHook(() => useSlackOAuth(defaultOptions));

            // Advance past the 500ms setTimeout delay in the auto-connect effect
            await act(async () => {
                vi.advanceTimersByTime(500);
            });

            // Flush the fetch promise
            await act(async () => {
                await vi.advanceTimersByTimeAsync(0);
            });

            // Should clean URL and call fetch
            expect(mockReplaceState).toHaveBeenCalledWith({}, expect.any(String), '/settings/personal-notifications');
            expect(global.fetch).toHaveBeenCalledWith('/integrations/slack/connect', expect.any(Object));

            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });
        });

        it('does not auto-connect when autoConnectEnabled is false', async () => {
            Object.defineProperty(window.location, 'search', {
                value: '?connect_slack=true',
                configurable: true,
            });

            renderHook(() => useSlackOAuth({ ...defaultOptions, autoConnectEnabled: false }));

            // Advance timers well past the 500ms delay
            await act(async () => {
                vi.advanceTimersByTime(1000);
            });

            expect(global.fetch).not.toHaveBeenCalled();

            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });
        });

        it('reports shouldAutoConnect correctly', () => {
            Object.defineProperty(window.location, 'search', {
                value: '?connect_slack=true',
                configurable: true,
            });

            const { result } = renderHook(() => useSlackOAuth(defaultOptions));
            expect(result.current.shouldAutoConnect).toBe(true);

            Object.defineProperty(window.location, 'search', {
                value: '',
                configurable: true,
            });
        });
    });
});
