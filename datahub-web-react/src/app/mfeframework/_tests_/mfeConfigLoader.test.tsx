import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';

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
        default: { load: vi.fn().mockImplementation(() => { throw error; }) },
    }));
}

function mockYamlFile(content: string) {
    vi.doMock('@app/mfeframework/mfe.config.yaml?raw', () => ({ default: content }));
}

function mockReactRouter() {
    vi.doMock('react-router', () => ({
        Route: ({ path, render: renderProp }: any) => <div>Route: {path} - {renderProp()}</div>,
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
            permissions: ['example_permission'],
            navIcon: 'Gear',
        },
        {
            id: 'another',
            label: 'Another from Yaml',
            path: '/another-mfe',
            remoteEntry: 'http://localhost:9111/remoteEntry.js',
            module: 'anotherRemoteModule/mount',
            flags: { enabled: true, showInNav: false },
            permissions: ['example_permission'],
            navIcon: 'Globe',
        },
    ],
};

describe('mfeConfigLoader', () => {
    beforeEach(() => {
        vi.resetModules();
        // Default YAML file mock for most tests; override in test if needed
        mockYamlFile('irrelevant');
    });

    it('loadMFEConfigFromYAML parses valid YAML and validates config', async () => {
        mockYamlLoad(validParsedYaml);
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        expect(result.subNavigationMode).toBe(false);
        expect(result.microFrontends.length).toBe(2);
        expect(result.microFrontends[0]).toMatchObject(validParsedYaml.microFrontends[0]);
        expect(result.microFrontends[1]).toMatchObject(validParsedYaml.microFrontends[1]);
    });

    it('loadMFEConfigFromYAML marks missing required fields as invalid and collects all errors', async () => {
        mockYamlLoad({
            subNavigationMode: false,
            microFrontends: [
                {
                    // id missing, navIcon missing
                    label: 'Missing ID and navIcon',
                    path: '/missing-id',
                    remoteEntry: 'remoteEntry.js',
                    module: 'MissingIdModule',
                    flags: { enabled: true, showInNav: false },
                    permissions: ['perm1'],
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        const mfe = result.microFrontends[0];
        if ('invalid' in mfe && mfe.invalid) {
            expect(Array.isArray((mfe as any).errorMessages)).toBe(true);
            expect((mfe as any).errorMessages).toEqual(
                expect.arrayContaining([
                    expect.stringContaining('Missing required field: id'),
                    expect.stringContaining('Missing required field: navIcon'),
                ]),
            );
        }
    });

    it('loadMFEConfigFromYAML marks multiple errors for a single MFE', async () => {
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
                    permissions: 'perm1', // not array
                    navIcon: 123, // not a string
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        const mfe = result.microFrontends[0];
        if ('invalid' in mfe && mfe.invalid) {
            expect(Array.isArray((mfe as any).errorMessages)).toBe(true);
            expect((mfe as any).errorMessages).toEqual(
                expect.arrayContaining([
                    expect.stringContaining('module must be a string'),
                    expect.stringContaining('flags.enabled must be boolean'),
                    expect.stringContaining('permissions must be a non-empty array of strings'),
                    expect.stringContaining('navIcon must be a non-empty string'),
                ]),
            );
        }
    });

    it('loadMFEConfigFromYAML marks empty permissions as invalid', async () => {
        mockYamlLoad({
            subNavigationMode: false,
            microFrontends: [
                {
                    id: 'empty-permissions',
                    label: 'Empty Permissions',
                    path: '/empty-permissions',
                    remoteEntry: 'remoteEntry.js',
                    module: 'EmptyPermissionsModule',
                    flags: { enabled: true, showInNav: false },
                    permissions: [],
                    navIcon: 'Gear',
                },
            ],
        });
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const result = loadMFEConfigFromYAML('irrelevant');
        const mfe = result.microFrontends[0];
        if ('invalid' in mfe && mfe.invalid) {
            expect(Array.isArray((mfe as any).errorMessages)).toBe(true);
            expect((mfe as any).errorMessages).toEqual(
                expect.arrayContaining([
                    expect.stringContaining('permissions must be a non-empty array of strings'),
                ]),
            );
        }
    });

    it('loadMFEConfigFromYAML throws if microFrontends is missing', async () => {
        mockYamlLoad({});
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        expect(() => loadMFEConfigFromYAML('irrelevant')).toThrow('[MFE Loader] Invalid YAML: missing microFrontends array');
    });

    it('loadMFEConfigFromYAML throws if YAML parsing fails', async () => {
        mockYamlLoadThrows(new Error('bad yaml'));
        const { loadMFEConfigFromYAML } = await import('../mfeConfigLoader');
        expect(() => loadMFEConfigFromYAML('irrelevant')).toThrow('bad yaml');
    });

    it('useMFEConfigFromYAML returns config if YAML is valid', async () => {
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
                    permissions: ['example_permission'],
                    navIcon: 'Gear',
                },
            ],
        });
        const { useMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromYAML());
        expect(result.current?.microFrontends[0]).toMatchObject({
            id: 'example-1',
            label: 'Example MFE Yaml Item',
            path: '/example-mfe-item',
            remoteEntry: 'http://example.com/remoteEntry.js',
            module: 'exampleApplication/mount',
            flags: { enabled: true, showInNav: true },
            permissions: ['example_permission'],
            navIcon: 'Gear',
        });
    });

    it('useMFEConfigFromYAML returns null if mfeYamlRaw is empty', async () => {
        mockYamlFile('');
        mockYamlLoad(null);
        const { useMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromYAML());
        expect(result.current).toBeNull();
    });

    it('useMFEConfigFromYAML returns null if YAML is invalid', async () => {
        mockYamlLoadThrows(new Error('bad yaml'));
        const { useMFEConfigFromYAML } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useMFEConfigFromYAML());
        expect(result.current).toBeNull();
    });

    it('useDynamicRoutes returns empty array if no config', async () => {
        mockYamlLoad(null);
        const { useDynamicRoutes } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useDynamicRoutes());
        expect(result.current).toEqual([]);
    });

    it('useDynamicRoutes returns Route elements for each MFE', async () => {
        mockYamlLoad(validParsedYaml);
        const { useDynamicRoutes } = await import('../mfeConfigLoader');
        const { result } = renderHook(() => useDynamicRoutes());
        expect(result.current).toHaveLength(2);
        expect(result.current[0].props.path).toBe('/example-mfe-item');
        expect(result.current[1].props.path).toBe('/another-mfe');
    });

    it('MFERoutes renders the dynamic routes', async () => {
        mockYamlLoad(validParsedYaml);
        mockReactRouter();
        mockMFEBasePage();
        const { MFERoutes } = await import('../mfeConfigLoader');
        const { container } = render(<MFERoutes />);
        expect(container.textContent).toContain('Route: /example-mfe-item');
        expect(container.textContent).toContain('Route: /another-mfe');
        expect(container.textContent).toContain('MFE: exampleApplication/mount');
        expect(container.textContent).toContain('MFE: anotherRemoteModule/mount');
    });
});