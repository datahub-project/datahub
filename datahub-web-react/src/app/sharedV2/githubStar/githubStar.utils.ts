import { Theme } from '@conf/theme/types';

type ThemeMenuItem = Theme['content']['menu']['items'][number];

export function getThemeMenuItemDescription(item: ThemeMenuItem, showGithubStarPrompt: boolean): string {
    if (item.isGithubStarPrompt && !showGithubStarPrompt) return '';
    return item.description || '';
}
