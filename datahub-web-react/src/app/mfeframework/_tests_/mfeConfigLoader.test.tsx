import { render, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * Test Isolation and Mocking Strategy
 * -----------------------------------
 * Each test in this file sets up its own mocks for YAML parsing and file imports
 * using inline helper functions. This ensures:
 *   - Complete isolation between tests: no mock leakage or shared state.
 *   - Deterministic behavior: each test controls exactly what the YAML parser and file return.
 *   - No external file reads: all YAML content is mocked, never loaded from disk.
 *   - Flexible mocking: UI dependencies and config variations are easily handled per test.
 * This approach keeps tests robust, maintainable, and focused on the intended scenario.
 */

function mockYamlLoad(returnValue: any) {
    vi.doMock('js-yaml', () => ({
        default: { load: vi.fn().mockReturnValue(returnValue) },
    }));
}

function mockYamlLoadThrows(error: Error) {
    vi.doMock('js-yaml', () => ({
        default: {
            load: vi.fn().mockImplementation(() => {
                throw error;
            }),
        },
    }));
}

function mockReactRouter() {
    vi.doMock('react-router', () => ({
        Route: ({ path, render: renderProp, component: Component, ...rest }: any) => {
            let content: React.ReactNode = null;
            if (renderProp) {
                content = renderProp(rest);
            } else if (Component) {
                content = React.createElement(Component, rest);
            }
            return (
                <div>
                    Route: {path} - {content}
                </div>
            );
        },
        Switch: ({ children }: any) => <div>Switch: {children}</div>,
        useHistory: () => ({
            replace: vi.fn(),
            push: vi.fn(),
            goBack: vi.fn(),
            location: {},
        }),
    }));
}

function mockMFEBasePage() {
    vi.doMock('@app/mfeframework/MFEConfigurableContainer', () => ({
        MFEBaseConfigurablePage: ({ config }: { config: any }) => <div>MFE: {config.module}</div>,
    }));
}

const validParsedYaml = {
    subNavigationMode: false,
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

describe('mfeConfigLoader', () => {
    beforeEach(() => {
        vi.resetModules();
    });

    // Helper to mock fetch for YAML string
    function mockFetchYaml(yamlString: string) {
        global.fetch = vi.fn(() =>
            Promise.resolve(
                new Response(yamlString, {
                    status: 200,
                    headers: { 'Content-Type': 'text/plain' },
                }),
            ),
        ) as typeof global.fetch;
    }

    it('loadMFEConfigFromYAML parses valid YAML and validates config', async () => {
        mockYamlLoad(validParsedYaml);
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        expect(result.subNavigationMode).toBe(false);
        expect(result.microFrontends.length).toBe(2);
        expect(result.microFrontends[0]).toMatchObject(validParsedYaml.microFrontends[0]);
        expect(result.microFrontends[1]).toMatchObject(validParsedYaml.microFrontends[1]);
    });

    it('loadMFEConfigFromYAML filters out entries with missing required fields', async () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        mockYamlLoad({
            subNavigationMode: false,
            microFrontends: [
                {
                    // id missing, navIcon missing
                    label: 'Missing ID and navIcon',
                    path: '/missing-id',
                    remoteEntry: 'remoteEntry.js',
                    module: 'MissingIdModule/mount',
                    flags: { enabled: true, showInNav: false },
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        // Invalid entries should be filtered out
        expect(result.microFrontends.length).toBe(0);
        // Errors should be logged
        expect(consoleErrorSpy).toHaveBeenCalledWith(
            expect.stringContaining('[MFE Loader] Invalid config for entry'),
            expect.arrayContaining([
                expect.stringContaining('Missing required field: id'),
                expect.stringContaining('Missing required field: navIcon'),
            ]),
        );
        consoleErrorSpy.mockRestore();
    });

    it('loadMFEConfigFromYAML filters out entries with invalid module pattern', async () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        mockYamlLoad({
            subNavigationMode: false,
            microFrontends: [
                {
                    id: 'myId',
                    label: 'myLabel',
                    path: '/my-path',
                    remoteEntry: 'remoteEntry.js',
                    module: 'invalidmodule', // missing '/' separator
                    flags: { enabled: true, showInNav: false },
                    navIcon: 'Gear',
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        // Invalid entry should be filtered out
        expect(result.microFrontends.length).toBe(0);
        // Error should be logged for invalid module pattern
        expect(consoleErrorSpy).toHaveBeenCalledWith(
            expect.stringContaining('[MFE Loader] Invalid config for entry'),
            expect.arrayContaining([
                expect.stringContaining('module must be a string with pattern "moduleName/functionName"'),
            ]),
        );
        consoleErrorSpy.mockRestore();
    });

    it('loadMFEConfigFromYAML filters out entries with multiple validation errors', async () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        mockYamlLoad({
            subNavigationMode: false,
            microFrontends: [
                {
                    id: 'bad-flags',
                    label: 'Bad Flags',
                    path: '/bad-flags',
                    remoteEntry: 'remoteEntry.js',
                    module: 123, // not a string
                    flags: { enabled: 'yes', showInNav: false }, // enabled not boolean
                    navIcon: 123, // not a string
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        // Invalid entries should be filtered out
        expect(result.microFrontends.length).toBe(0);
        // All errors should be logged
        expect(consoleErrorSpy).toHaveBeenCalledWith(
            expect.stringContaining('[MFE Loader] Invalid config for entry'),
            expect.arrayContaining([
                expect.stringContaining('module must be a string'),
                expect.stringContaining('flags.enabled must be boolean'),
                expect.stringContaining('navIcon must be a non-empty string'),
            ]),
        );
        consoleErrorSpy.mockRestore();
    });

    it('loadMFEConfigFromYAML throws if microFrontends is missing', async () => {
        mockYamlLoad({});
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        expect(() => loadMFEConfigFromYAML('irrelevant')).toThrow(
            '[MFE Loader] Invalid YAML: missing microFrontends array',
        );
    });

    it('loadMFEConfigFromYAML throws if YAML parsing fails', async () => {
        mockYamlLoadThrows(new Error('bad yaml'));
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        expect(() => loadMFEConfigFromYAML('irrelevant')).toThrow('bad yaml');
    });

    it('useMFEConfigFromBackend returns config if YAML is valid', async () => {
        const yamlString = `
            subNavigationMode: false
            microFrontends:
              - id: example-1
                label: Example MFE Yaml Item
                path: /example-mfe-item
                remoteEntry: http://example.com/remoteEntry.js
                module: exampleApplication/mount
                flags:
                  enabled: true
                  showInNav: true
                navIcon: Gear
            `;

        mockFetchYaml(yamlString);
        mockYamlLoad({
            subNavigationMode: false,
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
            ],
        });
        const { useMFEConfigFromBackend } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromBackend());
        await waitFor(() => {
            expect(result.current?.microFrontends[0]).toMatchObject({
                id: 'example-1',
                label: 'Example MFE Yaml Item',
                path: '/example-mfe-item',
                remoteEntry: 'http://example.com/remoteEntry.js',
                module: 'exampleApplication/mount',
                flags: { enabled: true, showInNav: true },
                navIcon: 'Gear',
            });
        });
    });

    it('useMFEConfigFromBackend returns null if Yaml is empty', async () => {
        mockYamlLoad(null);
        const { useMFEConfigFromBackend } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromBackend());
        expect(result.current).toBeNull();
    });

    it('useMFEConfigFromBackend returns null if YAML is invalid', async () => {
        mockYamlLoadThrows(new Error('bad yaml'));
        const { useMFEConfigFromBackend } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromBackend());
        expect(result.current).toBeNull();
    });

    it('useDynamicRoutes returns empty array if no config', async () => {
        mockYamlLoad(null);
        const { useDynamicRoutes } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useDynamicRoutes());
        expect(result.current).toEqual([]);
    });

    it('useDynamicRoutes returns Route elements for each MFE', async () => {
        const yamlString = `
        subNavigationMode: false
        microFrontends:
          - id: example-1
            label: Example MFE Yaml Item
            path: /example-mfe-item
            remoteEntry: http://example.com/remoteEntry.js
            module: exampleApplication/mount
            flags:
              enabled: true
              showInNav: true
            navIcon: Gear
          - id: myapp
            label: myapp from Yaml
            path: /myapp-mfe
            remoteEntry: http://localhost:9111/remoteEntry.js
            module: myapp/mount
            flags:
              enabled: true
              showInNav: false
            navIcon: Globe
        `;
        mockFetchYaml(yamlString);
        mockYamlLoad(validParsedYaml);
        const { useDynamicRoutes } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useDynamicRoutes());
        await waitFor(() => {
            expect(result.current).toHaveLength(2);
        });
        expect(result.current[0].props.path).toBe('/mfe/example-mfe-item');
        expect(result.current[1].props.path).toBe('/mfe/myapp-mfe');
    });

    it('MFERoutes renders the dynamic routes', async () => {
        const yamlString = `
        subNavigationMode: false
        microFrontends:
          - id: example-1
            label: Example MFE Yaml Item
            path: /example-mfe-item
            remoteEntry: http://example.com/remoteEntry.js
            module: exampleApplication/mount
            flags:
              enabled: true
              showInNav: true
            navIcon: Gear
          - id: myapp
            label: myapp from Yaml
            path: /myapp-mfe
            remoteEntry: http://localhost:9111/remoteEntry.js
            module: myapp/mount
            flags:
              enabled: true
              showInNav: false
            navIcon: Globe
        `;
        mockFetchYaml(yamlString);
        mockYamlLoad(validParsedYaml);
        mockReactRouter();
        mockMFEBasePage();
        const { MFERoutes } = await import('../mfeConfigLoader');
        const { container } = render(<MFERoutes />);
        await waitFor(() => {
            expect(container.textContent).toContain('Route: /mfe/example-mfe-item');
            expect(container.textContent).toContain('Route: /mfe/myapp-mfe');
            expect(container.textContent).toContain('MFE: exampleApplication/mount');
            expect(container.textContent).toContain('MFE: myapp/mount');
        });
    });
});
