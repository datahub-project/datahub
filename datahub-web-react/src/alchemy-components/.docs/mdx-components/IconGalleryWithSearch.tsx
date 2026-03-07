import { Button, ButtonProps, Icon } from '@components';
import React, { useState } from 'react';

import { IconDisplayBlock, IconGrid, IconGridItem } from './components';

interface Props {
    icons: string[];
}

export const IconGalleryWithSearch = ({ icons }: Props) => {
    const [iconSet, setIconSet] = useState(icons);
    const [search, setSearch] = useState('');
    const [weight, setWeight] = useState('regular');

    const filteredIcons = iconSet.filter((icon) => icon.toLowerCase().includes(search.toLowerCase()));

    const arrows = [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'ArrowCircleDown',
        'ArrowCircleLeft',
        'ArrowCircleRight',
        'ArrowCircleUp',
        'CaretLeft',
        'CaretRight',
        'CaretUp',
        'CaretDown',
        'ArrowBendUpRight',
        'ArrowClockwise',
        'ArrowCounterClockwise',
        'ArrowSquareOut',
        'ArrowsLeftRight',
        'ArrowsDownUp',
        'Export',
        'FastForward',
        'Rewind',
        'SignIn',
        'SignOut',
        'Download',
        'Upload',
        'Share',
        'Shuffle',
        'Swap',
        'ArrowFatDown',
        'ArrowFatUp',
        'ArrowFatLeft',
        'ArrowFatRight',
    ];

    const dataViz = [
        'ChartBar',
        'ChartBarHorizontal',
        'ChartLine',
        'ChartLineUp',
        'ChartPie',
        'ChartPieSlice',
        'Table',
        'Rows',
        'Columns',
        'Graph',
        'TreeStructure',
        'Database',
        'Stack',
        'Funnel',
        'FunnelSimple',
        'Kanban',
        'GridFour',
        'PresentationChart',
        'Gauge',
        'Activity',
    ];

    const social = [
        'User',
        'UserCircle',
        'Users',
        'UsersThree',
        'UsersFour',
        'UserPlus',
        'UserMinus',
        'UserGear',
        'ChatCircle',
        'ChatDots',
        'ChatTeardrop',
        'Chats',
        'Handshake',
        'Smiley',
        'SmileyMeh',
        'SmileySad',
        'Heart',
        'ThumbsUp',
        'ThumbsDown',
        'Star',
        'StarHalf',
        'Bell',
        'BellRinging',
    ];

    const notifs = [
        'Envelope',
        'EnvelopeOpen',
        'EnvelopeSimple',
        'Bell',
        'BellRinging',
        'BellSlash',
        'BellSimple',
        'Notification',
        'Tray',
        'Package',
    ];

    const handleChangeSet = (set) => {
        setIconSet(set);
        setSearch('');
    };

    const handleResetSet = () => {
        setIconSet(icons);
        setSearch('');
    };

    const smButtonProps: ButtonProps = {
        size: 'sm',
        color: 'gray',
    };

    return (
        <>
            <input
                type="search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search for an icon…"
                style={{ width: '100%', padding: '0.5rem', marginBottom: '0.5rem' }}
            />
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px' }}>
                <div style={{ display: 'flex', gap: '8px' }}>
                    <Button onClick={handleResetSet} {...smButtonProps}>
                        All
                    </Button>
                    <Button onClick={() => handleChangeSet(arrows)} {...smButtonProps}>
                        Arrows
                    </Button>
                    <Button onClick={() => handleChangeSet(dataViz)} {...smButtonProps}>
                        Data Viz
                    </Button>
                    <Button onClick={() => handleChangeSet(social)} {...smButtonProps}>
                        Social
                    </Button>
                    <Button onClick={() => handleChangeSet(notifs)} {...smButtonProps}>
                        Notifications
                    </Button>
                </div>
                <div style={{ display: 'flex', gap: '8px' }}>
                    <Button onClick={() => setWeight(weight === 'regular' ? 'fill' : 'regular')} {...smButtonProps}>
                        Weight: {weight === 'fill' ? 'Fill' : 'Regular'}
                    </Button>
                </div>
            </div>
            <IconGrid>
                {filteredIcons.map((icon) => (
                    <IconGridItem>
                        <IconDisplayBlock key={icon} title={icon}>
                            <Icon icon={icon} weight={weight as any} size="2xl" />
                        </IconDisplayBlock>
                        <span>{icon}</span>
                    </IconGridItem>
                ))}
            </IconGrid>
        </>
    );
};
