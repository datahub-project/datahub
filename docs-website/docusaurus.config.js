require("dotenv").config();
const isSaas = process.env.DOCUSAURUS_IS_SAAS === "true";

module.exports = {
  title: process.env.DOCUSAURUS_CONFIG_TITLE || "DataHub",
  tagline: "A Metadata Platform for the Modern Data Stack",
  url: process.env.DOCUSAURUS_CONFIG_URL || "https://datahubproject.io",
  baseUrl: process.env.DOCUSAURUS_CONFIG_BASE_URL || "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "datahub-project", // Usually your GitHub org/user name.
  projectName: "datahub", // Usually your repo name.
  stylesheets: [
    "https://fonts.googleapis.com/css2?family=Manrope:wght@400;600&display=swap",
  ],
  noIndex: isSaas,
  customFields: {
    isSaas: isSaas,
  },
  themeConfig: {
    ...(!isSaas && {
      announcementBar: {
        id: "announcement",
        content: `
          <div style="
            display: flex;
            gap: 2em;
            justify-content: center;
            align-items: center;
        ">
          <div style="
            display: flex;
            align-items: center;
            gap: 1em;
          ">
          <img src="/img/acryl-logo-white-mark.svg" style="
            max-height: 35px;
          ">
          <p style="
            margin-bottom: 0;
          "><strong>Managed DataHub</strong><span> &nbsp;Acryl Data delivers an easy to consume DataHub platform for the enterprise</span></p></div>
          <a href="https://www.acryldata.io/datahub-beta" target="_blank" class="button button--primary">Sign up for Managed DataHub →</a>
          </div>
        `,
        backgroundColor: "#070707",
        textColor: "#ffffff",
        isCloseable: false,
      },
    }),
    navbar: {
      title: null,
      logo: {
        alt: "DataHub Logo",
        src: `img/${
          isSaas ? "acryl" : "datahub"
        }-logo-color-light-horizontal.svg`,
        srcDark: `img/${
          isSaas ? "acryl" : "datahub"
        }-logo-color-dark-horizontal.svg`,
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
          href: "https://blog.datahubproject.io/",
          label: "Blog",
          position: "right",
        },
        {
          href: "https://feature-requests.datahubproject.io/",
          label: "Feature Requests",
          position: "right",
        },
        {
          href: "https://feature-requests.datahubproject.io/roadmap",
          label: "Roadmap",
          position: "right",
        },
        {
          href: "https://slack.datahubproject.io",
          "aria-label": "Slack",
          position: "right",
          className: "item__icon item__slack",
        },
        {
          href: "https://github.com/datahub-project/datahub",
          "aria-label": "GitHub",
          position: "right",
          className: "item__icon item__github",
        },

        {
          href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
          "aria-label": "YouTube",
          position: "right",
          className: "item__icon item__youtube",
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
              label: "Blog",
              href: "https://blog.datahubproject.io/",
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
              href: "https://feature-requests.datahubproject.io/roadmap",
            },
            {
              label: "Contributing",
              to: "docs/contributing",
            },
            {
              label: "GitHub",
              href: "https://github.com/datahub-project/datahub",
            },
            {
              label: "Feature Requests",
              href: "https://feature-requests.datahubproject.io/",
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
    algolia: {
      appId: "RK0UG797F3",
      apiKey: "39d7eb90d8b31d464e309375a52d674f",
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
          ...(!isSaas && {
            editUrl: "https://github.com/datahub-project/datahub/blob/master/",
          }),
          numberPrefixParser: false,
          // TODO: make these work correctly with the doc generation
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: [
            isSaas
              ? require.resolve("./src/styles/acryl.scss")
              : require.resolve("./src/styles/datahub.scss"),
            require.resolve("./src/styles/global.scss"),
          ],
        },
      },
    ],
  ],
  plugins: [
    [
      "@docusaurus/plugin-ideal-image",
      { quality: 100, sizes: [320, 640, 1280, 1440, 1600] },
    ],
    "docusaurus-plugin-sass",
    [
      "docusaurus-graphql-plugin",
      {
        schema: "./graphql/combined.graphql",
        routeBasePath: "/docs/graphql",
      },
    ],
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
