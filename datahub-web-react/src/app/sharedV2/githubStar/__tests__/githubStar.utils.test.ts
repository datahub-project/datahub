import { getThemeMenuItemDescription } from '@app/sharedV2/githubStar/githubStar.utils';

const GITHUB_ITEM = {
    label: 'DataHub GitHub',
    path: 'https://github.com/datahub-project/datahub',
    shouldOpenInNewTab: true,
    description: '★ Star us on GitHub',
    isGithubStarPrompt: true,
};
const DOCS_ITEM = {
    label: 'DataHub Project',
    path: 'https://docs.datahub.com',
    shouldOpenInNewTab: true,
    description: 'Explore DataHub Project website',
};

describe('getThemeMenuItemDescription', () => {
    it('shows the GitHub star subtitle only when the flag is on', () => {
        expect(getThemeMenuItemDescription(GITHUB_ITEM, true)).toBe('★ Star us on GitHub');
        expect(getThemeMenuItemDescription(GITHUB_ITEM, false)).toBe('');
    });

    it('leaves other subtitles alone', () => {
        expect(getThemeMenuItemDescription(DOCS_ITEM, false)).toBe('Explore DataHub Project website');
    });
});
