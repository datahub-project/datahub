import { useQuery } from '@tanstack/react-query';
import yaml from 'js-yaml';
import React, { useMemo } from 'react';
import { Route } from 'react-router';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';

export interface MFEFlags {
    enabled: boolean;
    showInNav: boolean;
}

// MFEConfig: Type for a valid micro frontend config entry.
export interface MFEConfig {
    id: string;
    label: string;
    path: string;
    remoteEntry: string;
    module: string;
    flags: MFEFlags;
    navIcon: string;
}

// MFESchema: The overall config schema.
export interface MFESchema {
    subNavigationMode: boolean;
    microFrontends: MFEConfig[];
}

const REQUIRED_FIELDS: (keyof MFEConfig)[] = ['id', 'label', 'path', 'remoteEntry', 'module', 'flags', 'navIcon'];

/**
 * validateMFEConfig:
 * - Validates a single micro frontend config entry.
 * - Collects all validation errors for the entry.
 * - Returns the valid MFEConfig if no errors, otherwise returns null and logs all errors.
 */
export function validateMFEConfig(config: any): MFEConfig | null {
    const errors: string[] = [];

    REQUIRED_FIELDS.forEach((field) => {
        if (config[field] === undefined || config[field] === null) {
            errors.push(`[MFE Loader] Missing required field: ${field}`);
        }
    });
    if (typeof config.id !== 'string') errors.push('[MFE Loader] id must be a string');
    if (typeof config.label !== 'string') errors.push('[MFE Loader] label must be a string');
    if (typeof config.path !== 'string' || !config.path.startsWith('/'))
        errors.push('[MFE Loader] path must be a string starting with "/"');
    if (typeof config.remoteEntry !== 'string') errors.push('[MFE Loader] remoteEntry must be a string');
    if (typeof config.module !== 'string') errors.push('[MFE Loader] module must be a string');
    if (typeof config.flags !== 'object' || config.flags === null) errors.push('[MFE Loader] flags must be an object');
    if (config.flags) {
        if (typeof config.flags.enabled !== 'boolean') errors.push('[MFE Loader] flags.enabled must be boolean');
        if (typeof config.flags.showInNav !== 'boolean') errors.push('[MFE Loader] flags.showInNav must be boolean');
    }
    if (typeof config.navIcon !== 'string' || !config.navIcon.length) {
        errors.push('[MFE Loader] navIcon must be a non-empty string');
    }

    // If any errors, log them and return null
    if (errors.length > 0) {
        console.error(`[MFE Loader] Invalid config for entry (id: ${config.id || 'unknown'}):`, errors);
        return null;
    }
    // Otherwise, return as valid MFEConfig
    return config as MFEConfig;
}

/**
 * loadMFEConfigFromYAML:
 * - Loads and parses the YAML config string.
 * - Validates each micro frontend entry, logging errors for invalid entries.
 * - Returns the parsed schema with only valid entries.
 * - Throws only if the overall YAML is malformed or missing the microFrontends array.
 */
export function loadMFEConfigFromYAML(yamlString: string): MFESchema {
    try {
        console.log('[MFE Loader] Raw YAML:', yamlString);
        const parsed = yaml.load(yamlString) as MFESchema;
        // console.log('[MFE Loader] Parsed YAML config:', parsed);
        if (!parsed || !Array.isArray(parsed.microFrontends)) {
            console.error('[MFE Loader] Invalid YAML: missing microFrontends array:', parsed);
            throw new Error('[MFE Loader] Invalid YAML: missing microFrontends array');
        }
        // Validate each entry, filtering out invalid ones
        parsed.microFrontends = parsed.microFrontends
            .map(validateMFEConfig)
            .filter((config): config is MFEConfig => config !== null);
        return parsed;
    } catch (e) {
        console.error('[MFE Loader] Error parsing YAML:', e);
        throw e;
    }
}

/**
 * fetchMFEConfig:
 * - Fetches and parses the MFE configuration from the backend.
 * - Returns the parsed and validated MFESchema.
 */
async function fetchMFEConfig(): Promise<MFESchema> {
    const response = await fetch('/api/mfe/config');
    if (!response.ok) throw new Error(`Failed to fetch YAML: ${response.statusText}`);
    const yamlText = await response.text();
    console.log('[MFE Loader] Fetched YAML: ', yamlText);
    return loadMFEConfigFromYAML(yamlText);
}

/**
 * useMFEConfigFromBackend:
 * - Uses TanStack Query to fetch and cache the MFE configuration.
 * - Returns data, isLoading, and error states.
 */
export function useMFEConfigFromBackend() {
    const { data, isLoading, error } = useQuery({
        queryKey: ['mfeConfig'],
        queryFn: fetchMFEConfig,
        staleTime: 10 * 60 * 1000, // 10 minutes - MFE config rarely changes
    });

    return { data, isLoading, error };
}

export function useDynamicRoutes() {
    const { data: mfeConfig, isLoading, error } = useMFEConfigFromBackend();

    const routes = useMemo(() => {
        if (!mfeConfig) return [];
        return mfeConfig.microFrontends.map((mfe) => (
            <Route key={mfe.path} path={`/mfe${mfe.path}`} render={() => <MFEBaseConfigurablePage config={mfe} />} />
        ));
    }, [mfeConfig]);

    return { routes, isLoading, error };
}

export const MFERoutes = () => {
    const { routes } = useDynamicRoutes();
    console.log('[DynamicRoute] Generated Routes:', routes);
    return <>{routes}</>;
};
