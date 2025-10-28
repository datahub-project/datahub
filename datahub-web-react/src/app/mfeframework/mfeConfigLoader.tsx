import yaml from 'js-yaml';
import React, { useMemo } from 'react';
import { Route } from 'react-router';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
// Vite's ?raw import lets you import the YAML file as a string
import mfeYamlRaw from '@app/mfeframework/mfe.config.yaml?raw';

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
    permissions: string[];
    navIcon: string;
}

/**
 * InvalidMFEConfig: Type for an invalid micro frontend config entry.
 * - invalid: true
 * - errorMessages: array of validation errors for this entry
 * - id: optional, for easier debugging/logging
 * - [key: string]: any; allows for partial/invalid configs
 */
export interface InvalidMFEConfig {
    invalid: true;
    errorMessages: string[];
    id?: string;
    [key: string]: any;
}

// MFEConfigEntry: Union type for either a valid or invalid config entry.
export type MFEConfigEntry = MFEConfig | InvalidMFEConfig;

// MFESchema: The overall config schema, with a union array for microFrontends.
export interface MFESchema {
    subNavigationMode: boolean;
    microFrontends: MFEConfigEntry[];
}

// SPECIAL NOTE: 'permissions' will be implemented in later sprints. Keeping it as required and subsequently validated so we do not forget.
const REQUIRED_FIELDS: (keyof MFEConfig)[] = [
    'id',
    'label',
    'path',
    'remoteEntry',
    'module',
    'flags',
    'permissions',
    'navIcon',
];

/**
 * validateMFEConfig:
 * - Validates a single micro frontend config entry.
 * - Collects all validation errors for the entry.
 * - Returns a valid MFEConfig if no errors, otherwise returns InvalidMFEConfig with all error messages.
 * - This allows the loader to keep all entries (valid and invalid) and not throw on the first error.
 */
export function validateMFEConfig(config: any): MFEConfigEntry {
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
    if (
        !Array.isArray(config.permissions) ||
        config.permissions.length === 0 ||
        !config.permissions.every((p) => typeof p === 'string')
    ) {
        errors.push('[MFE Loader] permissions must be a non-empty array of strings');
    }
    if (typeof config.navIcon !== 'string' || !config.navIcon.length) {
        errors.push('[MFE Loader] navIcon must be a non-empty string');
    }

    // If any errors, return as InvalidMFEConfig (with all errors collected)
    if (errors.length > 0) {
        return {
            ...config,
            invalid: true,
            errorMessages: errors,
        };
    }
    // Otherwise, return as valid MFEConfig
    return config as MFEConfig;
}

/**
 * loadMFEConfigFromYAML:
 * - Loads and parses the YAML config string.
 * - Validates each micro frontend entry, collecting errors but not throwing for individual entries.
 * - Returns the parsed schema with both valid and invalid entries.
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
        // Validate each entry, keeping both valid and invalid ones
        parsed.microFrontends = parsed.microFrontends.map(validateMFEConfig);
        return parsed;
    } catch (e) {
        console.error('[MFE Loader] Error parsing YAML:', e);
        throw e;
    }
}

export function useMFEConfigFromYAML(): MFESchema | null {
    return useMemo(() => {
        try {
            if (!mfeYamlRaw) {
                console.warn('[MFE Loader] No YAML config found');
                return null;
            }
            const config = loadMFEConfigFromYAML(mfeYamlRaw);
            if (config) {
                console.log('[MFE Loader] useMFEConfigFromYAML loaded:', config);
            }
            return config;
        } catch (e) {
            console.error('[MFE Loader] Config error:', e);
            return null; // <-- Return null so the app can continue rendering
        }
    }, []);
}

export function useDynamicRoutes(): JSX.Element[] {
    const mfeConfig = useMFEConfigFromYAML();

    return useMemo(() => {
        if (!mfeConfig) {
            console.warn('[DynamicRoute] No MFE config available');
            return [];
        }
        console.log('[DynamicRoute] MFE Config:', mfeConfig);
        // Only include valid MFEs in routes
        return mfeConfig.microFrontends
            .filter((mfe) => !('invalid' in mfe && mfe.invalid))
            .map((mfe) => (
                <Route key={mfe.path} path={mfe.path} render={() => <MFEBaseConfigurablePage config={mfe} />} />
            ));
    }, [mfeConfig]);
}

// Constant to store the dynamic routes hook
export const MFERoutes = () => {
    const routes = useDynamicRoutes();
    console.log('[DynamicRoute] Generated Routes:', routes);
    return <>{routes}</>;
};
