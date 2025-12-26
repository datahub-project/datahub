import yaml from 'js-yaml';
import React, { useEffect, useState } from 'react';
import { Route, Switch } from 'react-router';

import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
import { NoPageFound } from '@app/shared/NoPageFound';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

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
    // Must look like 'myRemoteModule/mount' which is exposed remote followed by exposed mount function inside it
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
    if (typeof config.module !== 'string' || !config.module.includes('/'))
        errors.push('[MFE Loader] module must be a string with pattern "moduleName/functionName"');
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

export function useMFEConfigFromBackend(): MFESchema | null {
    const [config, setConfig] = useState<MFESchema | null>(null);

    useEffect(() => {
        async function fetchConfig() {
            try {
                const response = await fetch(resolveRuntimePath('/mfe/config'));
                if (!response.ok) throw new Error(`Failed to fetch YAML: ${response.statusText}`);
                const yamlText = await response.text();

                if (import.meta.env.DEV) {
                    console.log('[MFE Loader] Fetched YAML: ', yamlText);
                }
                const parsedConfig = loadMFEConfigFromYAML(yamlText);
                setConfig(parsedConfig);
            } catch (e) {
                console.error('[MFE Loader] Config error:', e);
                setConfig(null);
            }
        }
        fetchConfig();
    }, []);

    return config;
}

export function useDynamicRoutes(): JSX.Element[] {
    const mfeConfig = useMFEConfigFromBackend();
    if (!mfeConfig) return [];
    // TODO- Reintroduce useMemo() hook here. Make it work with getting yaml from api as a react hook.
    return mfeConfig.microFrontends.map((mfe) => (
        <Route key={mfe.path} path={`/mfe${mfe.path}`} exact render={() => <MFEBaseConfigurablePage config={mfe} />} />
    ));
}

export const MFERoutes = () => {
    const routes = useDynamicRoutes();
    console.log('[DynamicRoute] Generated Routes:', routes);
    if (routes.length === 0) {
        return null;
    }
    return (
        <Switch>
            {routes}
            <Route path="/*" component={NoPageFound} />
        </Switch>
    );
};
