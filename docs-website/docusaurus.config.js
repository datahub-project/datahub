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
  staticDirectories: ["static", "genStatic"],
  stylesheets: ["https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700&display=swap"],
  scripts: [
    {
      src: "https://tools.luckyorange.com/core/lo.js?site-id=28ea8a38",
      async: true,
      defer: true,
    },
    {
      src: "/scripts/rb2b.js",
      async: true,
      defer: true,
    }
  ],
  noIndex: isSaas,
  customFields: {
    isSaas: isSaas,
    markpromptProjectKey: process.env.DOCUSAURUS_MARKPROMPT_PROJECT_KEY || "0U6baUoEdHVV4fyPpr5pxcX3dFlAMEu9",
  },

  // See https://github.com/facebook/docusaurus/issues/4765
  // and https://github.com/langchain-ai/langchainjs/pull/1568
  webpack: {
    jsLoader: (isServer) => ({
      loader: require.resolve("swc-loader"),
      options: {
        jsc: {
          parser: {
            syntax: "typescript",
            tsx: true,
          },
          target: "es2017",
        },
        module: {
          type: isServer ? "commonjs" : "es6",
        },
      },
    }),
  },

  themeConfig: {
    colorMode: {
      disableSwitch: true,
      defaultMode: 'light',
    },
    navbar: {
      title: null,
      logo: {
        alt: "DataHub Logo",
        src: `img/${isSaas ? "acryl" : "datahub"}-logo-color-light-horizontal.svg`,
        srcDark: `img/${isSaas ? "acryl" : "datahub"}-logo-color-dark-horizontal.svg`,
      },
      items: [
        // {
        //   type: "dropdown",
        //   label: "Product",
        //   position: "right",
        //   items: [
        //     {
        //       href: "/",
        //       label: "DataHub Core",
        //     },
        //     {
        //       to: "/cloud",
        //       label: "DataHub Cloud",
        //     },    
        //   ],
        // },
        {
          to: "/integrations",
          activeBasePath: "integrations",
          label: "Integrations",
          position: "right",
        },
        {
          type: "dropdown",
          label: "Learn",
          position: "right",
          items: [
            {
              to: "/learn",
              label: "Use Cases",
            },
            {
              to: "/adoption-stories",
              label: "Adoption Stories",
            },
            {
              href: "https://blog.datahubproject.io/",
              label: "Blog",
            },
            {
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
              label: "YouTube",
            },
          ]
        },
        {
          to: "docs/",
          activeBasePath: "docs",
          label: "Docs",
          position: "right",
        },
        {
          type: "dropdown",
          label: "Community",
          position: "right",
          items: [
            {
              to: "/slack",
              label: "DataHub Slack",
            },
            {
              href: "https://forum.datahubproject.io/",
              label: "DataHub Discourse",
            },
            {
              to: "/events",
              label: "Upcoming Events",
            },
            {
              to: "/champions",
              label: "DataHub Champions",
            },
            {
              label: "Share Your Journey",
              href: "/customer-stories-survey",
            },
          ],
        },
        {
          href: "/slack",
          html: `
            <style>
              .slack-logo:hover {
                opacity: 0.8;
              }
            </style>
            <img class='slack-logo' src='https://upload.wikimedia.org/wikipedia/commons/d/d5/Slack_icon_2019.svg', alt='slack', height='20px' style='margin: 10px 0 0 0;'/>
          `,
          position: "right",
        },
        {
          href: "/cloud",
          html: "<div class='button button--primary button--md'>Try DataHub Cloud Free</div>",
          position: "right",
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
              to: "https://demo.datahubproject.io/",
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
      // https://docusaurus.io/docs/markdown-features/code-blocks#theming
      // theme: require("prism-react-renderer/themes/vsLight"),
      // darkTheme: require("prism-react-renderer/themes/vsDark"),
      additionalLanguages: ["ini", "java", "graphql", "shell-session"],
    },
    algolia: {
      appId: "RK0UG797F3",
      apiKey: "39d7eb90d8b31d464e309375a52d674f",
      indexName: "datahubproject",
      insights: true,
      contextualSearch: true,
      // debug: true,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
            lastVersion: "current",
              versions: {
                current: {
                  label: "Next",
                  banner: 'none',
                },
              },
          path: "genDocs",
          sidebarPath: require.resolve("./sidebars.js"),
          ...(!isSaas && {
            editUrl: "https://github.com/datahub-project/datahub/blob/master/",
          }),
          numberPrefixParser: false,
          // TODO: make these work correctly with the doc generation
          showLastUpdateAuthor: false,
          showLastUpdateTime: false,
        },
        blog: {
          blogTitle: "DataHub Learn",
          blogSidebarTitle: "DataHub Learn",
          blogDescription: "Learn about the hot topics in the data ecosystem and how DataHub can help you with your data journey.",
          path: "src/learn",
          routeBasePath: "learn",
          postsPerPage: "ALL",
          blogListComponent: "../src/learn/_components/LearnListPage",
        },
        theme: {
          customCss: [
            isSaas ? require.resolve("./src/styles/acryl.scss") : require.resolve("./src/styles/datahub.scss"),
            require.resolve("./src/styles/global.scss"),
            require.resolve("./src/styles/sphinx.scss"),
            require.resolve("./src/styles/config-table.scss"),
            require.resolve("./src/components/SecondNavbar/styles.module.scss"),
          ],
        },
        pages: {
          path: "src/pages",
          mdxPageComponent: "@theme/MDXPage",
        },
        googleTagManager: {
          containerId: 'GTM-WK28RLTG',
        },
      },
    ],
  ],
  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        createRedirects(existingPath) {
          if (existingPath.includes('/docs')) {
            return [
              existingPath.replace('/docs', '/docs/next'),
              existingPath.replace('/docs', '/docs/0.13.0'),
              existingPath.replace('/docs', '/docs/0.12.1'),
              existingPath.replace('/docs', '/docs/0.11.0'),
              existingPath.replace('/docs', '/docs/0.10.5'),
            ];
          }
          return undefined; // Return a falsy value: no redirect created
        },
      },
    ],
    ["@docusaurus/plugin-ideal-image", { quality: 100, sizes: [320, 640, 1280, 1440, 1600] }],
    "docusaurus-plugin-sass",
    [
      "docusaurus-graphql-plugin",
      {
        schema: "./graphql/combined.graphql",
        routeBasePath: "/docs/graphql",
      },
    ],
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
