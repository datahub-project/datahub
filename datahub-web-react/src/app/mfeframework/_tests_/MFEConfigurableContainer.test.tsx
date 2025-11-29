import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
import * as themeHooks from '@app/useIsThemeV2';
import * as navBarHooks from '@app/useShowNavBarRedesign';

// Mock theme and navbar hooks
vi.spyOn(themeHooks, 'useIsThemeV2').mockReturnValue(true);
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
        'box-shadow-navbar-redesign': '0 2px 8px rgba(0,0,0,0.15)',
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
            render(
                <MemoryRouter>
                    <ThemeProvider theme={sampleTheme as any}>
                        <MFEBaseConfigurablePage config={yaml} />
                    </ThemeProvider>
                </MemoryRouter>,
            );
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
            render(
                <MemoryRouter>
                    <ThemeProvider theme={sampleTheme as any}>
                        <MFEBaseConfigurablePage config={yaml} />
                    </ThemeProvider>
                </MemoryRouter>,
            );
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

        render(
            <MemoryRouter>
                <ThemeProvider theme={sampleTheme as any}>
                    <MFEBaseConfigurablePage config={yaml} />
                </ThemeProvider>
            </MemoryRouter>,
        );

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
