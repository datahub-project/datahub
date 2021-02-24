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
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Style Guide',
              to: 'docs/',
            },
            {
              label: 'Second Doc',
              to: 'docs/doc2/',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Stack Overflow',
              href: 'https://stackoverflow.com/questions/tagged/docusaurus',
            },
            {
              label: 'Discord',
              href: 'https://discordapp.com/invite/docusaurus',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/docusaurus',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: 'blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/facebook/docusaurus',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} My Project, Inc. Built with Docusaurus.`,
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
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
