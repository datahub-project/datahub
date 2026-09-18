import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import {
    __federation_method_getRemote as getRemote,
    __federation_method_setRemote as setRemote,
    __federation_method_unwrapDefault as unwrapModule,
} from 'virtual:__federation__';

import { useUserContext } from '@app/context/useUserContext';
import { ErrorComponent } from '@app/mfeframework/ErrorComponent';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { NavPageContext, SLOT_CONTRACT_VERSION, SlotContext } from '@app/mfeframework/slots/slotTypes';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const REMOTE_LOAD_TIMEOUT_MS = 5000;
const DEFAULT_MOUNT_MIN_HEIGHT = 480;

const MFEConfigurableContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => props.theme.colors.bg};
    padding: 16px;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 100%;
        margin: 5px;
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
}

async function mountMFE({
    config,
    ctx,
    containerElement,
    onError,
    aliveRef,
}: MountMFEParams): Promise<(() => void) | undefined> {
    const { module, remoteEntry } = config;
    const mountStart = performance.now();

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

        // Create a timeout promise that rejects in a few seconds
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(
                () => reject(new Error(`Timeout loading from remote ${remoteName}, module: ${modulePathWithDot}`)),
                REMOTE_LOAD_TIMEOUT_MS,
            );
        });

        // Race between getRemote and timeout
        const fetchStart = performance.now();
        if (import.meta.env.DEV) {
            console.log('[HOST] Attempting to load remote module with config:', remoteConfig);
        }
        const remoteModule = await Promise.race([getRemote(remoteName, modulePathWithDot), timeoutPromise]);
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
            return undefined;
        }
        if (!config.flags.enabled) {
            console.warn(
                '[HOST] skipping remote module loading for<config.id> because planning not to show it, enabled=false',
            );
            return undefined;
        }

        if (!containerElement) {
            console.warn('[HOST] ref is null (container div not in DOM');
            return undefined;
        }

        if (typeof maybeFn !== 'function') {
            if (import.meta.env.DEV) {
                console.warn('MFE id: ', config.id, ' Mounting failed');
                console.warn('[HOST] mount is not a function; got: ', maybeFn);
            }
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
        return typeof cleanup === 'function' ? cleanup : undefined;
    } catch (e) {
        if (import.meta.env.DEV) {
            console.log(`latency for unsuccessful MFE id: ${config.id}`, performance.now() - mountStart, 'ms');
            console.error('[HOST] import/mount failed:', e);
        }
        if (aliveRef.current) {
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
};

/**
 * Loads the remote for `config` into a bare container and calls its `mount(el, ctx)`.
 * Surface-agnostic: pages and slots wrap this with their own chrome.
 */
export const MFEMount = ({ config, ctx, minHeight = DEFAULT_MOUNT_MIN_HEIGHT }: MFEMountProps) => {
    const { t } = useTranslation('misc');
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
    }, [config, ctx, history]);

    if (hasError) {
        return <ErrorComponent message={t('mfeframework.notAvailableError', { label: config.label })} />;
    }
    if (!config.flags.enabled) {
        return <ErrorComponent message={t('mfeframework.disabledError', { label: config.label })} />;
    }

    return <div ref={box} data-testid="mfe-slot-container" data-mfe-id={config.id} style={{ minHeight }} />;
};

/** Full-page MFE for the `nav.page` slot, reached from the left navigation at /mfe<path>. */
export const MFEBaseConfigurablePage = ({ config }: { config: MFEConfig }) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const principal = useSlotPrincipal();
    const ctx = useMemo<NavPageContext>(
        () => ({ slot: 'nav.page', version: SLOT_CONTRACT_VERSION, ...(principal ? { principal } : {}) }),
        [principal],
    );

    return (
        <MFEConfigurableContainer $isShowNavBarRedesign={isShowNavBarRedesign} data-testid="mfe-configurable-container">
            <MFEMount config={config} ctx={ctx} />
        </MFEConfigurableContainer>
    );
};
