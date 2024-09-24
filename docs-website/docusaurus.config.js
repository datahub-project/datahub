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
  stylesheets: [
    "https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700&display=swap",
  ],
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
    },
  ],
  noIndex: isSaas,
  customFields: {
    isSaas: isSaas,
    markpromptProjectKey:
      process.env.DOCUSAURUS_MARKPROMPT_PROJECT_KEY ||
      "0U6baUoEdHVV4fyPpr5pxcX3dFlAMEu9",
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
    ...(!isSaas && {
      announcementBar: {
        id: "announcement",
        content:
          '<div><img src="/img/acryl-logo-white-mark.svg" /><p><strong>DataHub Cloud</strong><span> &nbsp;Acryl Data delivers an easy to consume DataHub platform for the enterprise</span></p></div> <a href="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup" target="_blank" class="button button--primary">Sign Up for DataHub Cloud&nbsp;→</a>',
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
          to: "/cloud",
          activeBasePath: "cloud",
          label: "Cloud",
          position: "right",
        },
        {
          to: "/learn",
          activeBasePath: "learn",
          label: "Learn",
          position: "right",
        },
        {
          to: "/integrations",
          activeBasePath: "integrations",
          label: "Integrations",
          position: "right",
        },
        {
          type: "dropdown",
          label: "Community",
          position: "right",
          items: [
            {
              to: "/slack",
              label: "Join Slack",
            },
            {
              href: "https://forum.datahubproject.io/",
              label: "Community Forum",
            },
            {
              to: "/events",
              label: "Events",
            },
            {
              to: "/champions",
              label: "Champions",
            },
            {
              label: "Share Your Journey",
              href: "/customer-stories-survey",
            },
          ],
        },
        {
          type: "dropdown",
          label: "Resources",
          position: "right",
          items: [
            {
              href: "https://demo.datahubproject.io/",
              label: "Demo",
            },
            {
              href: "https://blog.datahubproject.io/",
              label: "Blog",
            },
            {
              href: "https://feature-requests.datahubproject.io/roadmap",
              label: "Roadmap",
            },
            {
              href: "https://github.com/datahub-project/datahub",
              label: "GitHub",
            },
            {
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
              label: "YouTube",
            },
            {
              href: "/adoption-stories",
              label: "Adoption Stories",
            },
            {
              href: "https://www.youtube.com/playlist?list=PLdCtLs64vZvErAXMiqUYH9e63wyDaMBgg",
              label: "DataHub Basics",
            },
          ],
        },
        {
          type: "docsVersionDropdown",
          position: "left",
          dropdownActiveClassDisabled: true,
          dropdownItemsAfter: [
            {
              type: "html",
              value: '<hr class="dropdown-separator" style="margin: 0.4rem;">',
            },
            {
              type: "html",
              value:
                '<div class="dropdown__link"><b>Archived versions</b></div>',
            },
            {
              value: `
                   <a class="dropdown__link" href="https://docs-website-lzxh86531-acryldata.vercel.app/docs/features">0.13.0
                   <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                   </a>
                   `,
              type: "html",
            },
            {
              value: `
                   <a class="dropdown__link" href="https://docs-website-2uuxmgza2-acryldata.vercel.app/docs/features">0.12.1
                   <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                   </a>
                   `,
              type: "html",
            },
            {
              value: `
                   <a class="dropdown__link" href="https://docs-website-irpoe2osc-acryldata.vercel.app/docs/features">0.11.0
                   <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                   </a>
                   `,
              type: "html",
            },
            {
              value: `
                   <a class="dropdown__link" href="https://docs-website-1gv2yzn9d-acryldata.vercel.app/docs/features">0.10.5
                   <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                   </a>
                   `,
              type: "html",
            },
          ],
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
              banner: "none",
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
          blogDescription:
            "Learn about the hot topics in the data ecosystem and how DataHub can help you with your data journey.",
          path: "src/learn",
          routeBasePath: "learn",
          postsPerPage: "ALL",
          blogListComponent: "../src/learn/_components/LearnListPage",
        },
        theme: {
          customCss: [
            isSaas
              ? require.resolve("./src/styles/acryl.scss")
              : require.resolve("./src/styles/datahub.scss"),
            require.resolve("./src/styles/global.scss"),
            require.resolve("./src/styles/sphinx.scss"),
            require.resolve("./src/styles/config-table.scss"),
          ],
        },
        pages: {
          path: "src/pages",
          mdxPageComponent: "@theme/MDXPage",
        },
        googleTagManager: {
          containerId: "GTM-WK28RLTG",
        },
      },
    ],
  ],
  plugins: [
    [
      "@docusaurus/plugin-client-redirects",
      {
        createRedirects(existingPath) {
          if (existingPath.includes("/docs")) {
            return [
              existingPath.replace("/docs", "/docs/next"),
              existingPath.replace("/docs", "/docs/0.13.0"),
              existingPath.replace("/docs", "/docs/0.12.1"),
              existingPath.replace("/docs", "/docs/0.11.0"),
              existingPath.replace("/docs", "/docs/0.10.5"),
            ];
          }
          return undefined; // Return a falsy value: no redirect created
        },
      },
    ],
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
    [
      "@docusaurus/plugin-content-blog",
      {
        id: "webinars",
        blogTitle: "Webinars",
        blogDescription:
          "Our live or on-demand webinars can help you gain insights - from understanding Acryl Data to discovering how businesses leverage it to take back control of their data.",
        blogSidebarCount: 0,
        path: "src/webinars",
        routeBasePath: "webinars",
        postsPerPage: "ALL",
        blogListComponent: "../src/webinars/_components/WebinarListPage",
        blogPostComponent: "../src/webinars/_components/WebinarPostPage",
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
