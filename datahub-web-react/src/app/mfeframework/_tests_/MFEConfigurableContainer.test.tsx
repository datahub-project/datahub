import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
import * as navBarHooks from '@app/useShowNavBarRedesign';

// Mock theme and navbar hooks
vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(true);

const validParsedYaml = {
    microFrontends: [
        {
            id: 'example-1',
            label: 'Example MFE Yaml Item',
            path: '/example-mfe-item',
            remoteEntry: 'http://example.com/remoteEntry.js',
            module: 'exampleApplication/mount',
            flags: { enabled: true, showInNav: true },
            navIcon: 'Gear',
        },
        {
            id: 'myapp',
            label: 'myapp from Yaml',
            path: '/myapp-mfe',
            remoteEntry: 'http://localhost:9111/remoteEntry.js',
            module: 'myapp/mount',
            flags: { enabled: true, showInNav: false },
            navIcon: 'Globe',
        },
    ],
};

const sampleTheme = {
    styles: {
        'border-radius-navbar-redesign': '16px',
        'box-shadow-navbar-redesign': 'none',
    },
    colors: {
        bg: 'none',
    },
    assets: {},
    content: {},
};

// Mock useHistory
const pushMock = vi.fn();
vi.mock('react-router-dom', async () => {
    const actual = await vi.importActual<any>('react-router-dom');
    return {
        ...actual,
        useHistory: () => ({ push: pushMock }),
    };
});

// Mock the Module Federation runtime API
const { registerRemotesMock, loadRemoteMock } = vi.hoisted(() => ({
    registerRemotesMock: vi.fn(),
    loadRemoteMock: vi.fn(),
}));

vi.mock('@module-federation/enhanced/runtime', () => ({
    registerRemotes: registerRemotesMock,
    loadRemote: loadRemoteMock,
}));

// Capture tracked events; EventType resolves each member to its own name so events can be matched by string.
const { trackEventMock } = vi.hoisted(() => ({ trackEventMock: vi.fn() }));
vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { event: trackEventMock },
    EventType: new Proxy({}, { get: (_target, key) => String(key) }),
}));

describe('MFEBaseConfigurablePage', () => {
    // Restore real timers even when a fake-timer test fails, so they can't leak into later tests.
    afterEach(() => {
        vi.useRealTimers();
    });

    beforeEach(() => {
        vi.clearAllMocks();
    });

    const renderPage = (yaml: any, loadTimeoutMs?: number) => {
        render(
            <MemoryRouter>
                <ThemeProvider theme={sampleTheme as any}>
                    <MFEBaseConfigurablePage config={yaml} loadTimeoutMs={loadTimeoutMs} />
                </ThemeProvider>
            </MemoryRouter>,
        );
    };

    it('renders the container div', () => {
        const yaml = validParsedYaml.microFrontends[0];
        render(
            <MemoryRouter>
                <ThemeProvider theme={sampleTheme as any}>
                    <MFEBaseConfigurablePage config={yaml} />
                </ThemeProvider>
            </MemoryRouter>,
        );
        const container = screen.getByTestId('mfe-configurable-container');
        expect(container).toBeInTheDocument();
    });

    it('calls mount on dynamic import', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        // Setup mocks for federation methods
        const mountFn = vi.fn(() => vi.fn()); // returns cleanup function
        loadRemoteMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderPage(yaml);
        });
        const container = screen.getByTestId('mfe-configurable-container');
        expect(container).toBeInTheDocument();
    });

    it('actually calls the mount function with the container and options', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        const mountFn = vi.fn(() => vi.fn());
        loadRemoteMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderPage(yaml);
        });

        const container = screen.getByTestId('mfe-configurable-container');
        const mountTarget = container.querySelector('div');
        expect(mountFn).toHaveBeenCalledWith(mountTarget, {});
    });

    it('registers the remote as a webpack var container and loads the exposed module', async () => {
        const yaml = validParsedYaml.microFrontends[1];
        const mountFn = vi.fn(() => vi.fn());
        loadRemoteMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderPage(yaml);
        });

        expect(registerRemotesMock).toHaveBeenCalledWith(
            [{ name: 'myapp', entry: 'http://localhost:9111/remoteEntry.js', type: 'var', entryGlobalName: 'myapp' }],
            { force: false },
        );
        expect(loadRemoteMock).toHaveBeenCalledWith('myapp/mount');
        expect(mountFn).toHaveBeenCalled();
    });

    it('does not re-register an unchanged remote and force-registers a changed entry', async () => {
        const yaml = { ...validParsedYaml.microFrontends[1], id: 'myapp-2', module: 'myappTwo/mount' };
        loadRemoteMock.mockResolvedValue({ mount: vi.fn() });

        await act(async () => {
            renderPage(yaml);
        });
        await act(async () => {
            renderPage(yaml);
        });
        expect(registerRemotesMock).toHaveBeenCalledTimes(1);

        const movedYaml = { ...yaml, remoteEntry: 'http://localhost:9222/remoteEntry.js' };
        await act(async () => {
            renderPage(movedYaml);
        });
        expect(registerRemotesMock).toHaveBeenCalledTimes(2);
        expect(registerRemotesMock).toHaveBeenLastCalledWith(
            [
                {
                    name: 'myappTwo',
                    entry: 'http://localhost:9222/remoteEntry.js',
                    type: 'var',
                    entryGlobalName: 'myappTwo',
                },
            ],
            { force: true },
        );
    });

    it('mounts the default export of an ES module remote', async () => {
        const yaml = { ...validParsedYaml.microFrontends[0], id: 'esm-default' };
        const mountFn = vi.fn(() => vi.fn());
        loadRemoteMock.mockResolvedValue({ __esModule: true, default: mountFn });

        await act(async () => {
            renderPage(yaml);
        });

        expect(mountFn).toHaveBeenCalledWith(screen.getByTestId('mfe-configurable-container').querySelector('div'), {});
    });

    it('shows error UI when the remote fails to load', async () => {
        const yaml = { ...validParsedYaml.microFrontends[0], id: 'broken' };
        loadRemoteMock.mockRejectedValue(new Error('RUNTIME-008: failed to load script resources'));

        await act(async () => {
            renderPage(yaml);
        });

        expect(screen.getByText(`${yaml.label} is not available at this time`)).toBeInTheDocument();
    });

    it('shows error UI when remote module times out', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        // Mock loadRemote to never resolve
        loadRemoteMock.mockImplementation(() => new Promise(() => {}));

        vi.useFakeTimers();

        renderPage(yaml);

        // Advance timers to trigger timeout
        act(() => {
            vi.advanceTimersByTime(10000);
        });

        // Wait for the error message to appear with a timeout
        await vi.waitFor(
            () => {
                expect(screen.getByText(`${yaml.label} is not available at this time`)).toBeInTheDocument();
            },
            { timeout: 1000 },
        );

        vi.useRealTimers();
    }, 10000); // Increase test timeout

    it('uses the loadTimeoutMs resolved from the MFE config yaml as the remote load timeout', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        // Mock loadRemote to never resolve so only the timeout can settle the race
        loadRemoteMock.mockImplementation(() => new Promise(() => {}));

        vi.useFakeTimers();

        renderPage(yaml, 1000);

        // Just under the configured timeout: no error yet
        await act(async () => {
            vi.advanceTimersByTime(999);
        });
        expect(screen.queryByText(`${yaml.label} is not available at this time`)).toBeNull();

        // Crossing the configured timeout triggers the error state well before the 10000ms default
        act(() => {
            vi.advanceTimersByTime(1);
        });
        await vi.waitFor(
            () => {
                expect(screen.getByText(`${yaml.label} is not available at this time`)).toBeInTheDocument();
            },
            { timeout: 1000 },
        );

        vi.useRealTimers();
    }, 10000);

    it('falls back to a 10000ms load timeout when the yaml specifies no loadTimeoutMs', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        loadRemoteMock.mockImplementation(() => new Promise(() => {}));

        vi.useFakeTimers();

        renderPage(yaml, undefined);

        // Just under the default timeout: no error yet
        await act(async () => {
            vi.advanceTimersByTime(9999);
        });
        expect(screen.queryByText(`${yaml.label} is not available at this time`)).toBeNull();

        act(() => {
            vi.advanceTimersByTime(1);
        });
        await vi.waitFor(
            () => {
                expect(screen.getByText(`${yaml.label} is not available at this time`)).toBeInTheDocument();
            },
            { timeout: 1000 },
        );

        vi.useRealTimers();
    }, 10000);

    it('tracks a page view and a successful load', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderPage(yaml, 5000);
        });

        expect(trackEventMock).toHaveBeenCalledWith({
            type: 'MfePageViewEvent',
            mfeId: yaml.id,
            mfeLabel: yaml.label,
            mfePath: yaml.path,
        });
        expect(trackEventMock).toHaveBeenCalledWith(
            expect.objectContaining({
                type: 'MfeLoadEvent',
                mfeId: yaml.id,
                outcome: 'success',
                loadTimeoutMs: 5000,
                durationMs: expect.any(Number),
            }),
        );
        expect(trackEventMock.mock.calls.filter(([event]) => event.type === 'MfeLoadEvent')).toHaveLength(1);
    });

    it('tracks a timed-out load', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        getRemoteMock.mockImplementation(() => new Promise(() => {}));

        vi.useFakeTimers();
        renderPage(yaml, 1000);
        await act(async () => {
            vi.advanceTimersByTime(1000);
        });

        await vi.waitFor(() => {
            expect(trackEventMock).toHaveBeenCalledWith(
                expect.objectContaining({ type: 'MfeLoadEvent', mfeId: yaml.id, outcome: 'timeout' }),
            );
        });
        vi.useRealTimers();
    });

    it('tracks a failed remote load', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        getRemoteMock.mockRejectedValue(new Error('remoteEntry.js returned 503'));

        await act(async () => {
            renderPage(yaml);
        });

        await vi.waitFor(() => {
            expect(trackEventMock).toHaveBeenCalledWith(
                expect.objectContaining({ type: 'MfeLoadEvent', mfeId: yaml.id, outcome: 'load_error' }),
            );
        });
    });

    it('tracks a load whose remote exposes no mount function', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        getRemoteMock.mockResolvedValue({});
        unwrapModuleMock.mockResolvedValue({ notMount: 42 });

        await act(async () => {
            renderPage(yaml);
        });

        await vi.waitFor(() => {
            expect(trackEventMock).toHaveBeenCalledWith(
                expect.objectContaining({ type: 'MfeLoadEvent', mfeId: yaml.id, outcome: 'invalid_mount' }),
            );
        });
    });

    it('shows error UI when enabled flag is false', async () => {
        const yaml = {
            ...validParsedYaml.microFrontends[0],
            flags: { enabled: false, showInNav: true },
        };

        render(
            <MemoryRouter>
                <ThemeProvider theme={sampleTheme as any}>
                    <MFEBaseConfigurablePage config={yaml} />
                </ThemeProvider>
            </MemoryRouter>,
        );

        // Check that the error message is displayed
        expect(screen.getByText(`${yaml.label} is disabled.`)).toBeInTheDocument();
    });
});
