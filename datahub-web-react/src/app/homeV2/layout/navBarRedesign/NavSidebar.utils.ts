import analytics, { EventType } from '@app/analytics';
import { NavBarMenuDropdownItemElement } from '@app/homeV2/layout/navBarRedesign/types';
import { getThemeMenuItemDescription } from '@app/sharedV2/githubStar/githubStar.utils';
import { Theme } from '@conf/theme/types';

type ThemeMenuItem = Theme['content']['menu']['items'][number];

function getGithubStarOnClick(item: ThemeMenuItem, showGithubStarPrompt: boolean): (() => void) | undefined {
    const { path } = item;
    if (!item.isGithubStarPrompt || !showGithubStarPrompt || !path) return undefined;
    return () => {
        analytics.event({ type: EventType.GithubStarClickEvent, sourceType: 'resourcesMenu' });
        window.open(path, '_blank', 'noopener,noreferrer');
    };
}

export function getHelpMenuItems(
    items: ThemeMenuItem[],
    showGithubStarPrompt: boolean,
): NavBarMenuDropdownItemElement[] {
    return items.map((value) => ({
        title: value.label,
        description: getThemeMenuItemDescription(value, showGithubStarPrompt),
        isDescriptionHighlighted: value.isGithubStarPrompt,
        link: value.path,
        isHidden: false,
        isExternalLink: true,
        key: `helpMenu${value.label}`,
        onClick: getGithubStarOnClick(value, showGithubStarPrompt),
    })) as NavBarMenuDropdownItemElement[];
}
