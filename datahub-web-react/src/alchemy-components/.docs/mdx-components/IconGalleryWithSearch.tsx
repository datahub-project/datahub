import React, { useState } from 'react';

import { Icon, Button, ButtonProps } from '@components';
import { IconGrid, IconGridItem, IconDisplayBlock } from './components';

interface Props {
    icons: string[];
}

export const IconGalleryWithSearch = ({ icons }: Props) => {
    const [iconSet, setIconSet] = useState(icons);
    const [search, setSearch] = useState('');
    const [variant, setVariant] = useState('outline');

    const filteredIcons = iconSet.filter((icon) => icon.toLowerCase().includes(search.toLowerCase()));

    const arrows = [
        'ArrowBack',
        'ArrowCircleDown',
        'ArrowCircleLeft',
        'ArrowCircleRight',
        'ArrowCircleUp',
        'ArrowDownward',
        'ArrowForward',
        'ArrowOutward',
        'ArrowUpward',
        'CloseFullscreen',
        'Cached',
        'Code',
        'CodeOff',
        'CompareArrows',
        'Compress',
        'ChevronLeft',
        'ChevronRight',
        'DoubleArrow',
        'FastForward',
        'FastRewind',
        'FileDownload',
        'FileUpload',
        'ForkLeft',
        'ForkRight',
        'GetApp',
        'LastPage',
        'Launch',
        'Login',
        'Logout',
        'LowPriority',
        'ManageHistory',
        'Merge',
        'MergeType',
        'MoveUp',
        'MultipleStop',
        'OpenInFull',
        'Outbound',
        'Outbox',
        'Output',
        'PlayArrow',
        'PlayCircle',
        'Publish',
        'ReadMore',
        'ExitToApp',
        'Redo',
        'Refresh',
        'Replay',
        'ReplyAll',
        'Reply',
        'Restore',
        'SaveAlt',
        'Shortcut',
        'SkipNext',
        'SkipPrevious',
        'Start',
        'Straight',
        'SubdirectoryArrowLeft',
        'SubdirectoryArrowRight',
        'SwapHoriz',
        'SwapVert',
        'SwitchLeft',
        'SwitchRight',
        'SyncAlt',
        'SyncDisabled',
        'SyncLock',
        'Sync',
        'Shuffle',
        'SyncProblem',
        'TrendingDown',
        'TrendingFlat',
        'TrendingUp',
        'TurnLeft',
        'TurnRight',
        'TurnSlightLeft',
        'TurnSlightRight',
        'Undo',
        'UnfoldLessDouble',
        'UnfoldLess',
        'UnfoldMoreDouble',
        'UnfoldMore',
        'UpdateDisabled',
        'Update',
        'Upgrade',
        'Upload',
        'ZoomInMap',
        'ZoomOutMap',
    ];

    const dataViz = [
        'AccountTree',
        'Analytics',
        'ArtTrack',
        'Article',
        'BackupTable',
        'BarChart',
        'BubbleChart',
        'Calculate',
        'Equalizer',
        'List',
        'FormatListBulleted',
        'FormatListNumbered',
        'Grading',
        'InsertChart',
        'Hub',
        'Insights',
        'Lan',
        'Leaderboard',
        'LegendToggle',
        'Map',
        'MultilineChart',
        'Nat',
        'PivotTableChart',
        'Poll',
        'Polyline',
        'QueryStats',
        'Radar',
        'Route',
        'Rule',
        'Schema',
        'Sort',
        'SortByAlpha',
        'ShowChart',
        'Source',
        'SsidChart',
        'StackedBarChart',
        'StackedLineChart',
        'Storage',
        'TableChart',
        'TableRows',
        'TableView',
        'Timeline',
        'ViewAgenda',
        'ViewArray',
        'ViewCarousel',
        'ViewColumn',
        'ViewComfy',
        'ViewCompact',
        'ViewCozy',
        'ViewDay',
        'ViewHeadline',
        'ViewKanban',
        'ViewList',
        'ViewModule',
        'ViewQuilt',
        'ViewSidebar',
        'ViewStream',
        'ViewTimeline',
        'ViewWeek',
        'Visibility',
        'VisibilityOff',
        'Webhook',
        'Window',
    ];

    const social = [
        'AccountCircle',
        'Badge',
        'Campaign',
        'Celebration',
        'Chat',
        'ChatBubble',
        'CommentBank',
        'Comment',
        'CommentsDisabled',
        'Message',
        'ContactPage',
        'Contacts',
        'GroupAdd',
        'Group',
        'GroupRemove',
        'Groups',
        'Handshake',
        'ManageAccounts',
        'MoodBad',
        'SentimentDissatisfied',
        'SentimentNeutral',
        'SentimentSatisfied',
        'Mood',
        'NoAccounts',
        'People',
        'PersonAddAlt1',
        'PersonOff',
        'Person',
        'PersonRemoveAlt1',
        'PersonSearch',
        'SwitchAccount',
        'StarBorder',
        'StarHalf',
        'Star',
        'ThumbDown',
        'ThumbUp',
        'ThumbsUpDown',
        'Verified',
        'VerifiedUser',
    ];

    const notifs = [
        'Mail',
        'Drafts',
        'MarkAsUnread',
        'Inbox',
        'Outbox',
        'MoveToInbox',
        'Unsubscribe',
        'Upcoming',
        'NotificationAdd',
        'NotificationImportant',
        'NotificationsActive',
        'NotificationsOff',
        'Notifications',
        'NotificationsPaused',
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
                    <Button onClick={() => setVariant(variant === 'outline' ? 'filled' : 'outline')} {...smButtonProps}>
                        Variant: {variant === 'filled' ? 'Filled' : 'Outline'}
                    </Button>
                </div>
            </div>
            <IconGrid>
                {filteredIcons.map((icon) => (
                    <IconGridItem>
                        <IconDisplayBlock key={icon} title={icon}>
                            <Icon icon={icon} variant={variant as any} size="2xl" />
                        </IconDisplayBlock>
                        <span>{icon}</span>
                    </IconGridItem>
                ))}
            </IconGrid>
        </>
    );
};
