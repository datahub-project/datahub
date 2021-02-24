module.exports = {
  title: 'DataHub',
  tagline: 'A Generalized Metadata Search & Discovery Tool',
  url: 'https://your-docusaurus-test-site.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'linkedin', // Usually your GitHub org/user name.
  projectName: 'datahub', // Usually your repo name.
  themeConfig: {
    navbar: {
      title: 'DataHub',
      logo: {
        alt: 'DataHub Logo',
        src: 'img/logo-color.png',
        srcDark: 'img/logo-dark.png',
      },
      items: [
        {
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'Docs',
          position: 'right',
        },
        {
          to: 'https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA',
          label: 'Slack',
          position: 'right',
        },
        {
          href: 'https://github.com/linkedin/datahub',
          label: 'GitHub',
          position: 'right',
        },
        // TODO: add search
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Introduction',
              to: 'docs/',
            },
            {
              label: 'Quickstart',
              to: 'docs/quickstart',
            },
            {
              label: 'Features',
              to: 'docs/features',
            },
            {
              label: 'FAQs',
              to: 'docs/faq',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Slack',
              href: 'https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA',
            },
            {
              label: 'Town Halls',
              to: 'docs/townhalls',
            },
            {
              label: 'Adoption',
              to: 'docs/#adoption',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Roadmap',
              to: 'docs/roadmap',
            },
            {
              label: 'Contributing',
              to: 'docs/contributing',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/facebook/docusaurus',
            },
          ],
        },
      ],
      copyright: `Copyright © 2015-${new Date().getFullYear()} DataHub Project Authors. Built with Docusaurus.`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          path: 'genDocs',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/linkedin/datahub/blob/master/'
        },
        blog: false,
        theme: {
          // TODO: add custom colors?
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
