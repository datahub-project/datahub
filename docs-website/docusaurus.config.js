module.exports = {
  title: "DataHub",
  tagline: "A Metadata Platform for the Modern Data Stack",
  url: "https://datahubproject.io",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "datahub-project", // Usually your GitHub org/user name.
  projectName: "datahub", // Usually your repo name.
  stylesheets: [
    "https://fonts.googleapis.com/css2?family=Manrope:wght@400;600&display=swap",
  ],
  themeConfig: {
    colorMode: {
      switchConfig: {
        darkIcon: " ",
        darkIconStyle: {
          backgroundImage: `url("data:image/svg+xml,%3Csvg width='18' height='18' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M15.556 9.784A6.065 6.065 0 0 1 14 9.987a5.948 5.948 0 0 1-4.235-1.752 6.02 6.02 0 0 1-1.548-5.792.75.75 0 0 0-.918-.917A7.51 7.51 0 0 0 3.93 3.462c-2.924 2.924-2.924 7.682 0 10.607a7.453 7.453 0 0 0 5.304 2.198c2.003 0 3.886-.78 5.302-2.197a7.505 7.505 0 0 0 1.937-3.368.75.75 0 0 0-.918-.918Zm-2.079 3.225a5.96 5.96 0 0 1-4.242 1.758 5.964 5.964 0 0 1-4.243-1.758 6.009 6.009 0 0 1 0-8.486 5.942 5.942 0 0 1 1.545-1.112 7.52 7.52 0 0 0 2.167 5.886 7.479 7.479 0 0 0 5.886 2.168 6.026 6.026 0 0 1-1.113 1.544Z' fill='%23fff' opacity='.5'/%3E%3C/svg%3E")`,
          width: "18px",
          height: "18px",
          margin: "-4px 0 0 -4px",
        },
        lightIcon: " ",
        lightIconStyle: {
          backgroundImage: `url("data:image/svg+xml,%3Csvg width='18' height='18' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cg opacity='.5' fill='%23fff'%3E%3Cpath d='M5.245 9A3.76 3.76 0 0 0 9 12.755 3.76 3.76 0 0 0 12.755 9 3.76 3.76 0 0 0 9 5.245 3.76 3.76 0 0 0 5.245 9ZM9 6.745A2.258 2.258 0 0 1 11.255 9 2.258 2.258 0 0 1 9 11.255 2.258 2.258 0 0 1 6.745 9 2.258 2.258 0 0 1 9 6.745Zm-.752 7.505h1.5v2.25h-1.5v-2.25Zm0-12.75h1.5v2.25h-1.5V1.5Zm-6.75 6.75h2.25v1.5h-2.25v-1.5Zm12.75 0h2.25v1.5h-2.25v-1.5ZM3.164 13.773l1.59-1.592 1.062 1.06-1.59 1.592-1.062-1.06ZM12.18 4.758l1.592-1.592 1.06 1.06-1.591 1.592-1.06-1.06ZM4.756 5.819l-1.59-1.592 1.06-1.06L5.818 4.76l-1.06 1.06ZM14.832 13.773l-1.06 1.06-1.592-1.591 1.06-1.06 1.592 1.59Z'/%3E%3C/g%3E%3C/svg%3E")`,
          width: "18px",
          height: "18px",
          margin: "-4px 0 0 -4px",
        },
      },
    },
    announcementBar: {
      id: "announcement",
      content:
        '<div><img src="/img/acryl-logo-white-mark.svg" /><p><strong>Managed DataHub</strong><span> &nbsp;Acryl Data delivers an easy to consume DataHub platform for the enterprise</span></p></div> <a href="https://www.acryldata.io/datahub-beta" target="_blank" class="button button--primary">Sign up for Managed DataHub →</a>',
      backgroundColor: "#090a11",
      textColor: "#ffffff",
      isCloseable: false,
    },
    navbar: {
      title: null,
      logo: {
        alt: "DataHub Logo",
        src: "img/datahub-logo-color-light-horizontal.svg",
        srcDark: "img/datahub-logo-color-dark-horizontal.svg",
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
          editUrl: "https://github.com/datahub-project/datahub/blob/master/",
          numberPrefixParser: false,
          // TODO: make these work correctly with the doc generation
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/styles/global.scss"),
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
