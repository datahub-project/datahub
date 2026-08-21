import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
import * as navBarHooks from '@app/useShowNavBarRedesign';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';

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

// Mock federation methods
const { setRemoteMock } = vi.hoisted(() => ({ setRemoteMock: vi.fn() }));
const { getRemoteMock } = vi.hoisted(() => ({ getRemoteMock: vi.fn() }));
const { unwrapModuleMock } = vi.hoisted(() => ({ unwrapModuleMock: vi.fn() }));

vi.mock('virtual:__federation__', () => ({
    __federation_method_getRemote: getRemoteMock,
    __federation_method_setRemote: setRemoteMock,
    __federation_method_unwrapDefault: unwrapModuleMock,
}));

describe('MFEBaseConfigurablePage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    // The page only mounts the MFE once appConfig has loaded, so tests must render inside a
    // provider with loaded: true (the bare context default is loaded: false).
    const renderWithAppConfig = (yaml: any, mfeLoadTimeoutMs?: number) => {
        const config = {
            ...DEFAULT_APP_CONFIG,
            dataHubConfig: { ...DEFAULT_APP_CONFIG.dataHubConfig, mfeLoadTimeoutMs },
        };
        render(
            <AppConfigContext.Provider value={{ config, loaded: true, refreshContext: () => null }}>
                <MemoryRouter>
                    <ThemeProvider theme={sampleTheme as any}>
                        <MFEBaseConfigurablePage config={yaml} />
                    </ThemeProvider>
                </MemoryRouter>
            </AppConfigContext.Provider>,
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
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderWithAppConfig(yaml);
        });
        const container = screen.getByTestId('mfe-configurable-container');
        expect(container).toBeInTheDocument();
    });

    it('actually calls the mount function with the container and options', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });

        await act(async () => {
            renderWithAppConfig(yaml);
        });

        const container = screen.getByTestId('mfe-configurable-container');
        const mountTarget = container.querySelector('div');
        expect(mountFn).toHaveBeenCalledWith(mountTarget, {});
    });

    it('shows error UI when remote module times out', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        // Mock getRemote to never resolve
        getRemoteMock.mockImplementation(() => new Promise(() => {}));
        unwrapModuleMock.mockResolvedValue({});

        vi.useFakeTimers();

        renderWithAppConfig(yaml);

        // Advance timers to trigger timeout
        act(() => {
            vi.advanceTimersByTime(5000);
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

    it('uses dataHubConfig.mfeLoadTimeoutMs from app config as the remote load timeout', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        // Mock getRemote to never resolve so only the timeout can settle the race
        getRemoteMock.mockImplementation(() => new Promise(() => {}));
        unwrapModuleMock.mockResolvedValue({});

        vi.useFakeTimers();

        renderWithAppConfig(yaml, 1000);

        // Just under the configured timeout: no error yet
        await act(async () => {
            vi.advanceTimersByTime(999);
        });
        expect(screen.queryByText(`${yaml.label} is not available at this time`)).toBeNull();

        // Crossing the configured timeout triggers the error state well before the 5000ms default
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

    it('falls back to a 5000ms load timeout when the server does not provide mfeLoadTimeoutMs', async () => {
        const yaml = validParsedYaml.microFrontends[0];
        getRemoteMock.mockImplementation(() => new Promise(() => {}));
        unwrapModuleMock.mockResolvedValue({});

        vi.useFakeTimers();

        renderWithAppConfig(yaml, undefined);

        // Just under the default timeout: no error yet
        await act(async () => {
            vi.advanceTimersByTime(4999);
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
