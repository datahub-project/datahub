import { getHelpMenuItems } from '@app/homeV2/layout/navBarRedesign/NavSidebar.utils';

const ITEMS = [
    {
        label: 'DataHub Project',
        path: 'https://docs.datahub.com',
        shouldOpenInNewTab: true,
        description: 'Explore DataHub Project website',
    },
    {
        label: 'DataHub GitHub',
        path: 'https://github.com/datahub-project/datahub',
        shouldOpenInNewTab: true,
        description: '★ Star us on GitHub',
        isGithubStarPrompt: true,
    },
];

describe('getHelpMenuItems', () => {
    it('shows the highlighted GitHub star subtitle when the flag is on', () => {
        const [docs, github] = getHelpMenuItems(ITEMS, true);

        expect(github.description).toBe('★ Star us on GitHub');
        expect(github.isDescriptionHighlighted).toBe(true);
        expect(docs.description).toBe('Explore DataHub Project website');
    });

    it('hides only the GitHub star subtitle when the flag is off, keeping the link', () => {
        const [docs, github] = getHelpMenuItems(ITEMS, false);

        expect(github.description).toBe('');
        expect(github.link).toBe('https://github.com/datahub-project/datahub');
        expect(docs.description).toBe('Explore DataHub Project website');
    });
});
