import React, { useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import {
    __federation_method_getRemote as getRemote,
    __federation_method_setRemote as setRemote,
    __federation_method_unwrapDefault as unwrapModule,
} from 'virtual:__federation__';

import { ErrorComponent } from '@app/mfeframework/ErrorComponent';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const MFEConfigurableContainer = styled.div<{ isV2: boolean; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.isV2 ? '#fff' : 'inherit')};
    padding: 16px;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 100%;
        margin: 5px;
        overflow: auto;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-right: ${props.isV2 ? '24px' : '0'};
        margin-bottom: ${props.isV2 ? '24px' : '0'};
    `}
    border-radius: ${(props) => {
        if (props.isV2 && props.$isShowNavBarRedesign) return props.theme.styles['border-radius-navbar-redesign'];
        return props.isV2 ? '8px' : '0';
    }};
`;

interface MountMFEParams {
    config: MFEConfig;
    containerElement: HTMLDivElement | null;
    onError: () => void;
    aliveRef: { current: boolean };
}

async function mountMFE({
    config,
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
                5000,
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
        const cleanup = maybeFn(containerElement, {});
        const mountFnEnd = performance.now();
        if (import.meta.env.DEV) {
            console.log(`latency for mount function execution: ${config.id}`, mountFnEnd - mountFnStart, 'ms');
            console.log('[HOST] mount called');
        }
        const mountEnd = performance.now();
        const latency = mountEnd - mountStart;
        if (import.meta.env.DEV) {
            console.log(`latency for successful MFE id: ${config.id}`, latency, 'ms');
        }
        return cleanup;
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

export const MFEBaseConfigurablePage = ({ config }: { config: MFEConfig }) => {
    const isV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const box = useRef<HTMLDivElement>(null);
    const history = useHistory();
    const [hasError, setHasError] = useState(false);
    const aliveRef = useRef(true);

    useEffect(() => {
        aliveRef.current = true;
        let cleanup: (() => void) | undefined;

        mountMFE({
            config,
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
    }, [config, history]);

    if (hasError) {
        return <ErrorComponent isV2={isV2} message={`${config.label} is not available at this time`} />;
    }
    if (!config.flags.enabled) {
        return <ErrorComponent isV2={isV2} message={`${config.label} is disabled.`} />;
    }

    return (
        <MFEConfigurableContainer
            isV2={isV2}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            data-testid="mfe-configurable-container"
        >
            <div ref={box} style={{ minHeight: 480 }} />
        </MFEConfigurableContainer>
    );
};
