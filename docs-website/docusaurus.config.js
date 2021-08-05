module.exports = {
  title: "DataHub",
  tagline: "A Metadata Platform for the Modern Data Stack",
  url: "https://datahubproject.io",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "linkedin", // Usually your GitHub org/user name.
  projectName: "datahub", // Usually your repo name.
  themeConfig: {
    navbar: {
      title: "DataHub",
      logo: {
        alt: "DataHub Logo",
        src: "img/logo-color.png",
        srcDark: "img/logo-dark.png",
      },
      items: [
        {
          to: "docs/",
          activeBasePath: "docs",
          label: "Docs",
          position: "right",
        },
        {
          to: "docs/demo",
          label: "Demo",
          position: "right",
        },
        {
          href: "https://slack.datahubproject.io",
          label: "Slack",
          position: "right",
        },
        {
          href: "https://github.com/linkedin/datahub",
          label: "GitHub",
          position: "right",
        },
        {
          to: "docs/saas",
          label: "SaaS",
          position: "right",
          className: "navbar-saas-button button button--primary",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Introduction",
              to: "docs/",
            },
            {
              label: "Quickstart",
              to: "docs/quickstart",
            },
            {
              label: "Features",
              to: "docs/features",
            },
            {
              label: "FAQs",
              to: "docs/faq",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Slack",
              href: "https://slack.datahubproject.io",
            },
            {
              label: "YouTube",
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
            },
            {
              label: "Town Halls",
              to: "docs/townhalls",
            },
            {
              label: "Adoption",
              to: "docs/#adoption",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "Demo",
              to: "docs/demo",
            },
            {
              label: "Roadmap",
              to: "docs/roadmap",
            },
            {
              label: "Contributing",
              to: "docs/contributing",
            },
            {
              label: "GitHub",
              href: "https://github.com/linkedin/datahub",
            },
          ],
        },
      ],
      copyright: `Copyright © 2015-${new Date().getFullYear()} DataHub Project Authors.`,
    },
    prism: {
      //   theme: require('prism-react-renderer/themes/github'),
      //   darkTheme: require('prism-react-renderer/themes/dracula'),
      additionalLanguages: ["ini"],
    },
    gtag: {
      trackingID: "G-2G54RXWD4D",
    },
    algolia: {
      apiKey: "26a4b687e96e7476b5a6f11365a83336",
      indexName: "datahubproject",
      // contextualSearch: true,
      // searchParameters: {},
      // debug: true,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          path: "genDocs",
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/linkedin/datahub/blob/master/",
          // TODO: make these work correctly with the doc generation
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  plugins: [
    "@docusaurus/plugin-ideal-image",
    // '@docusaurus/plugin-google-gtag',
    // [
    //   require.resolve("@easyops-cn/docusaurus-search-local"),
    //   {
    //     // `hashed` is recommended as long-term-cache of index file is possible.
    //     hashed: true,
    //     language: ["en"],
    //     docsDir: "genDocs",
    //     blogDir: [],
    //   },
    // ],
  ],
};
