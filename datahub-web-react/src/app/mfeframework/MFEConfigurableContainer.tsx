import React, { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import {
    __federation_method_getRemote as getRemote,
    __federation_method_setRemote as setRemote,
    __federation_method_unwrapDefault as unwrapModule,
} from 'virtual:__federation__';

import analytics, { EventType, MfeLoadOutcome } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ErrorComponent } from '@app/mfeframework/ErrorComponent';
import { MFEConfigContext } from '@app/mfeframework/MFEConfigContext';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';
import { SlotContext } from '@app/mfeframework/slots/slotTypes';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

// Used when the MFE config yaml specifies no loadTimeoutMs.
export const DEFAULT_LOAD_TIMEOUT_MS = 10000;
const DEFAULT_MOUNT_MIN_HEIGHT = 480;

const MFEConfigurableContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => props.theme.colors.bg};
    padding: 16px;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 100%;
        overflow: auto;
        box-shadow: ${props.theme.colors.shadowSm};
    `}
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: 24px;
        margin-bottom: 24px;
    `}
    border-radius: ${(props) => {
        if (props.$isShowNavBarRedesign) return props.theme.styles['border-radius-navbar-redesign'];
        return '8px';
    }};
`;

interface MountMFEParams {
    config: MFEConfig;
    ctx: SlotContext;
    containerElement: HTMLDivElement | null;
    onError: () => void;
    aliveRef: { current: boolean };
    loadTimeoutMs: number;
}

async function mountMFE({
    config,
    ctx,
    containerElement,
    onError,
    aliveRef,
    loadTimeoutMs,
}: MountMFEParams): Promise<(() => void) | undefined> {
    const { module, remoteEntry } = config;
    const mountStart = performance.now();
    let timedOut = false;

    // Reports how the load ended. Called exactly once per mountMFE call, on every return path.
    const trackLoad = (outcome: MfeLoadOutcome) => {
        analytics.event({
            type: EventType.MfeLoadEvent,
            mfeId: config.id,
            mfeLabel: config.label,
            outcome,
            durationMs: Math.round(performance.now() - mountStart),
            loadTimeoutMs,
        });
    };

    if (import.meta.env.DEV) {
        console.log('MFE id: ', config.id, ' Mounting start ');
    }
    try {
        if (import.meta.env.DEV) {
            console.log('[HOST] mount path: ', module);
            console.log('[HOST] attempting mount');
        }

        // Parse module string, something like: "myapp/mount"
        const [remoteName, modulePath] = module.split('/');
        const modulePathWithDot = `./${modulePath}`; // Convert "mount" to "./mount"

        if (import.meta.env.DEV) {
            console.log('[HOST] parsed remote name: ', remoteName);
            console.log('[HOST] parsed module path: ', modulePathWithDot);
        }

        // Configure the dynamic remote
        const remoteConfig = {
            url: remoteEntry,
            format: 'var' as const,
            from: 'webpack' as const,
        };
        setRemote(remoteName, remoteConfig);

        // Create a timeout promise that rejects once the configured load timeout elapses
        let timeoutHandle: number | undefined;
        const timeoutPromise = new Promise((_, reject) => {
            timeoutHandle = window.setTimeout(() => {
                timedOut = true;
                reject(new Error(`Timeout loading from remote ${remoteName}, module: ${modulePathWithDot}`));
            }, loadTimeoutMs);
        });

        // Race between getRemote and timeout
        const fetchStart = performance.now();
        if (import.meta.env.DEV) {
            console.log('[HOST] Attempting to load remote module with config:', remoteConfig);
        }
        // Clear the timer once the race settles so it doesn't outlive a successful load or an unmount.
        const remoteModule = await Promise.race([getRemote(remoteName, modulePathWithDot), timeoutPromise]).finally(
            () => window.clearTimeout(timeoutHandle),
        );
        const fetchEnd = performance.now();
        if (import.meta.env.DEV) {
            console.log(`latency for remote module fetch: ${config.id}`, fetchEnd - fetchStart, 'ms');
            console.log('[HOST] Remote module loaded, unwrapping...');
        }
        const unwrapStart = performance.now();
        const mod = await unwrapModule(remoteModule);
        const unwrapEnd = performance.now();
        if (import.meta.env.DEV) {
            console.log(`latency for module unwrap: ${config.id}`, unwrapEnd - unwrapStart, 'ms');
            console.log('[HOST] imported mod: ', mod);
            console.log('[HOST] mod type: ', typeof mod);
        }

        const maybeFn =
            typeof mod === 'function'
                ? mod
                : ((mod as any)?.mount ??
                  (typeof (mod as any)?.default === 'function' ? (mod as any).default : (mod as any)?.default?.mount));

        if (!aliveRef.current) {
            console.error('[HOST] import/mount has failed due to timeout.');
            trackLoad('abandoned');
            return undefined;
        }
        if (!config.flags.enabled) {
            console.warn(
                '[HOST] skipping remote module loading for<config.id> because planning not to show it, enabled=false',
            );
            trackLoad('disabled');
            return undefined;
        }

        if (!containerElement) {
            console.warn('[HOST] ref is null (container div not in DOM');
            trackLoad('container_missing');
            return undefined;
        }

        if (typeof maybeFn !== 'function') {
            if (import.meta.env.DEV) {
                console.warn('MFE id: ', config.id, ' Mounting failed');
                console.warn('[HOST] mount is not a function; got: ', maybeFn);
            }
            trackLoad('invalid_mount');
            return undefined;
        }
        const mountFnStart = performance.now();
        const cleanup = maybeFn(containerElement, ctx);
        const mountFnEnd = performance.now();
        if (import.meta.env.DEV) {
            console.log(`latency for mount function execution: ${config.id}`, mountFnEnd - mountFnStart, 'ms');
            console.log('[HOST] mount called with ctx', ctx);
        }
        const mountEnd = performance.now();
        const latency = mountEnd - mountStart;
        if (import.meta.env.DEV) {
            console.log(`latency for successful MFE id: ${config.id}`, latency, 'ms');
        }
        trackLoad('success');
        return typeof cleanup === 'function' ? cleanup : undefined;
    } catch (e) {
        if (import.meta.env.DEV) {
            console.log(`latency for unsuccessful MFE id: ${config.id}`, performance.now() - mountStart, 'ms');
            console.error('[HOST] import/mount failed:', e);
        }
        if (!aliveRef.current) {
            trackLoad('abandoned');
        } else {
            trackLoad(timedOut ? 'timeout' : 'load_error');
            onError();
        }
        return undefined;
    }
}

/** Optional principal for any slot context, derived from the authenticated user. */
export function useSlotPrincipal(): { user: string } | undefined {
    const { urn } = useUserContext();
    return useMemo(() => (urn ? { user: urn } : undefined), [urn]);
}

type MFEMountProps = {
    config: MFEConfig;
    /** Typed context for the slot this MFE is placed in. Memoize it: a new object remounts the remote. */
    ctx: SlotContext;
    minHeight?: number;
    /** Remote-module load timeout. Omitted => the shared config's `loadTimeoutMs`, else the default. */
    loadTimeoutMs?: number;
};

/**
 * Loads the remote for `config` into a bare container and calls its `mount(el, ctx)`.
 * Surface-agnostic: pages and slots wrap this with their own chrome.
 */
export const MFEMount = ({
    config,
    ctx,
    minHeight = DEFAULT_MOUNT_MIN_HEIGHT,
    loadTimeoutMs: loadTimeoutMsProp,
}: MFEMountProps) => {
    const { t } = useTranslation('misc');
    // Slot hosts don't thread the timeout through, so fall back to the config every MFE shares.
    const { config: sharedConfig } = useContext(MFEConfigContext);
    const loadTimeoutMs = loadTimeoutMsProp ?? sharedConfig?.loadTimeoutMs ?? DEFAULT_LOAD_TIMEOUT_MS;
    const box = useRef<HTMLDivElement>(null);
    const history = useHistory();
    const [hasError, setHasError] = useState(false);
    const aliveRef = useRef(true);

    useEffect(() => {
        aliveRef.current = true;
        let cleanup: (() => void) | undefined;

        mountMFE({
            config,
            ctx,
            containerElement: box.current,
            onError: () => setHasError(true),
            aliveRef,
            loadTimeoutMs,
        }).then((cleanupFn) => {
            cleanup = cleanupFn;
        });

        return () => {
            aliveRef.current = false;
            if (cleanup) {
                if (import.meta.env.DEV) {
                    console.log('[HOST] Executing cleanup method provided by mount');
                }
                const cleanupStart = performance.now();
                cleanup();
                const cleanupEnd = performance.now();
                if (import.meta.env.DEV) {
                    console.log(`latency for cleanup execution: ${config.id}`, cleanupEnd - cleanupStart, 'ms');
                }
            }
        };
    }, [config, ctx, history, loadTimeoutMs]);

    if (hasError) {
        return <ErrorComponent message={t('mfeframework.notAvailableError', { label: config.label })} />;
    }
    if (!config.flags.enabled) {
        return <ErrorComponent message={t('mfeframework.disabledError', { label: config.label })} />;
    }

    return <div ref={box} data-testid="mfe-slot-container" data-mfe-id={config.id} style={{ minHeight }} />;
};

/** Full-page MFE for the `nav.page` slot, reached from the left navigation at /mfe<path>. */
interface MFEBaseConfigurablePageProps {
    config: MFEConfig;
    // Already resolved from the yaml by useDynamicRoutes (the yaml's loadTimeoutMs, else the default).
    loadTimeoutMs?: number;
}

export const MFEBaseConfigurablePage = ({
    config,
    loadTimeoutMs = DEFAULT_LOAD_TIMEOUT_MS,
}: MFEBaseConfigurablePageProps) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const principal = useSlotPrincipal();
    // Built by the builder registered for this entry's contract version. Entries with no `placement`
    // (every pre-slot config) resolve to nav.page on the default version, so legacy MFEs are unaffected.
    const ctx = useMemo(() => buildSlotContext(config, { principal }), [config, principal]);

    // A page view is a nav.page concept; slot mounts only report their load outcome (from mountMFE).
    useEffect(() => {
        analytics.event({
            type: EventType.MfePageViewEvent,
            mfeId: config.id,
            mfeLabel: config.label,
            mfePath: config.path ?? '',
        });
    }, [config]);

    // Gate 2 — the entry declared a contract version this host has no builder for; don't mount it.
    if (!ctx) return null;

    return (
        <MFEConfigurableContainer $isShowNavBarRedesign={isShowNavBarRedesign} data-testid="mfe-configurable-container">
            <MFEMount config={config} ctx={ctx} loadTimeoutMs={loadTimeoutMs} />
        </MFEConfigurableContainer>
    );
};
