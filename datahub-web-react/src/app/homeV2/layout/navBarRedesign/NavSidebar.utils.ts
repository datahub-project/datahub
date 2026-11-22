import { NavBarMenuDropdownItemElement } from '@app/homeV2/layout/navBarRedesign/types';
import { getThemeMenuItemDescription } from '@app/sharedV2/githubStar/githubStar.utils';
import { Theme } from '@conf/theme/types';

type ThemeMenuItem = Theme['content']['menu']['items'][number];

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
    })) as NavBarMenuDropdownItemElement[];
}
