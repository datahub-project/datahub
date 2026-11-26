import { Button, ButtonProps, Icon, PhosphorIconWeight } from '@components';
import React, { ComponentType, useEffect, useState } from 'react';

import { IconDisplayBlock, IconGrid, IconGridItem } from './components';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type IconModule = Record<string, ComponentType<any> | unknown>;

const WEIGHTS: PhosphorIconWeight[] = ['regular', 'thin', 'light', 'bold', 'fill', 'duotone'];

const smButtonProps: ButtonProps = {
    size: 'sm',
    color: 'gray',
};

interface Props {
    icons: string[];
}

export const IconGalleryWithSearch = ({ icons }: Props) => {
    const [iconModule, setIconModule] = useState<IconModule | null>(null);
    const [search, setSearch] = useState('');
    const [weight, setWeight] = useState<PhosphorIconWeight>('regular');
    const [loadFailed, setLoadFailed] = useState(false);

    useEffect(() => {
        // The gallery needs every icon, so like the app's IconPicker it loads the whole set in one
        // async chunk rather than using per-icon CSR imports.
        // eslint-disable-next-line rulesdir/no-phosphor-generic-imports
        import('@phosphor-icons/react')
            .then((mod) => setIconModule(mod as IconModule))
            .catch(() => setLoadFailed(true));
    }, []);

    const filteredIcons = icons.filter((icon) => icon.toLowerCase().includes(search.toLowerCase()));

    const cycleWeight = () => setWeight(WEIGHTS[(WEIGHTS.indexOf(weight) + 1) % WEIGHTS.length]);

    return (
        <>
            <input
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search for an icon…"
                style={{ width: '100%', padding: '0.5rem', marginBottom: '0.5rem' }}
            />
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '8px' }}>
                <span>
                    Showing {filteredIcons.length} of {icons.length}
                </span>
                <Button onClick={cycleWeight} {...smButtonProps}>
                    Weight: {weight}
                </Button>
            </div>
            {loadFailed && <p>Icons failed to load. Reload the page to try again.</p>}
            {!loadFailed && !iconModule && <p>Loading icons…</p>}
            {iconModule && (
                <IconGrid>
                    {filteredIcons.map((name) => {
                        const component = iconModule[name];
                        if (!component) return null;
                        return (
                            <IconGridItem key={name}>
                                <IconDisplayBlock title={name}>
                                    {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
                                    <Icon icon={component as ComponentType<any>} size="2xl" weight={weight} />
                                </IconDisplayBlock>
                                <span>{name}</span>
                            </IconGridItem>
                        );
                    })}
                </IconGrid>
            )}
        </>
    );
};
