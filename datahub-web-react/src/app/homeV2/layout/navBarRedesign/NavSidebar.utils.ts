import { NavBarMenuDropdownItemElement } from '@app/homeV2/layout/navBarRedesign/types';
import { Theme } from '@conf/theme/types';

type ThemeMenuItem = Theme['content']['menu']['items'][number];

export function getHelpMenuItems(
    items: ThemeMenuItem[],
    showGithubStarPrompt: boolean,
): NavBarMenuDropdownItemElement[] {
    return items.map((value) => {
        const isHiddenStarPrompt = value.isGithubStarPrompt && !showGithubStarPrompt;
        return {
            title: value.label,
            description: isHiddenStarPrompt ? '' : value.description || '',
            isDescriptionHighlighted: value.isGithubStarPrompt,
            link: value.path,
            isHidden: false,
            isExternalLink: true,
            key: `helpMenu${value.label}`,
        };
    }) as NavBarMenuDropdownItemElement[];
}
