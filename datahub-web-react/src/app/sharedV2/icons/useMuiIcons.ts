import { useEffect, useState } from 'react';

type MuiIconsModule = Record<string, React.ElementType>;

let cached: MuiIconsModule | null = null;
let pending: Promise<MuiIconsModule> | null = null;

function load(): Promise<MuiIconsModule> {
    if (cached) return Promise.resolve(cached);
    if (!pending) {
        pending = import('@mui/icons-material').then((mod) => {
            cached = mod as unknown as MuiIconsModule;
            return cached;
        });
    }
    return pending;
}

export function useMuiIcons(): MuiIconsModule | null {
    const [icons, setIcons] = useState<MuiIconsModule | null>(cached);

    useEffect(() => {
        if (!cached) {
            load().then(setIcons);
        }
    }, []);

    return icons;
}
