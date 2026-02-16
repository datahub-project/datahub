require("dotenv").config();
const isSaas = process.env.DOCUSAURUS_IS_SAAS === "true";

module.exports = {
  title: process.env.DOCUSAURUS_CONFIG_TITLE || "DataHub",
  tagline: "The #1 Open Source Metadata Platform",
  url: process.env.DOCUSAURUS_CONFIG_URL || "https://docs.datahub.com",
  baseUrl: process.env.DOCUSAURUS_CONFIG_BASE_URL || "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "datahub-project", // Usually your GitHub org/user name.
  projectName: "datahub", // Usually your repo name.
  staticDirectories: ["static"],
  stylesheets: ["https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700&display=swap"],
  headTags: [
    {
      tagName: 'meta',
      attributes: {
        httpEquiv: 'Content-Security-Policy',
        content: "frame-ancestors 'self' https://*.acryl.io https://acryldata.io http://localhost:*"
      }
    },
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
    {
      src: "https://app.revenuehero.io/scheduler.min.js"
    },
    {
      src: "https://tag.clearbitscripts.com/v1/pk_2e321cabe30432a5c44c0424781aa35f/tags.js",
      referrerPolicy: "strict-origin-when-cross-origin"
    },
    {
      src: "/scripts/reo.js",
    },
    {
      id: "runllm-widget-script",
      type: "module",
      src: "https://widget.runllm.com",
      crossorigin: "true",
      "runllm-name": "DataHub",
      "runllm-assistant-id": "81",
      "runllm-position": "BOTTOM_RIGHT",
      "runllm-keyboard-shortcut": "Mod+j",
      "runllm-preset": "docusaurus",
      "runllm-theme-color": "#1890FF",
      "runllm-brand-logo": "https://docs.datahub.com/img/datahub-logo-color-mark.svg",
      "runllm-community-url": "https://datahub.com/slack",
      "runllm-community-type": "slack",
      "runllm-disable-ask-a-person": "true",
      async: true,
    },
  ],
  noIndex: isSaas,
  customFields: {
    isSaas: isSaas,
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
    // ...(!isSaas && {
    //   announcementBar: {
    //     id: "announcement",
    //     content:
    //       '<div><img src="/img/acryl-logo-white-mark.svg" /><p><strong>DataHub Cloud</strong><span> &nbsp;DataHub delivers an easy to consume DataHub platform for the enterprise</span></p></div> <a href="https://www.datahub.com/demo?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup" target="_blank" class="button button--primary">Sign Up for DataHub Cloud&nbsp;→</a>',
    //     backgroundColor: "#070707",
    //     textColor: "#ffffff",
    //     isCloseable: false,
    //   },
    // }),
    announcementBar: {
      id: "announcement-3",
      content:
        '<div class="shimmer-banner"><span>February Town Hall 2/26</span><a href="https://luma.com/zp3h4ex8?utm_term=docs" target="_blank" class="button"><div>Register Here<span> →</span></div></a></div>',
      backgroundColor: "transparent",
      textColor: "#ffffff",
      isCloseable: false,
    },
    colorMode: {
      // Only support light mode.
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: null,
      logo: {
        alt: "DataHub Logo",
        src: `img/${isSaas ? "acryl" : "datahub"}-logo-color-light-horizontal.svg`,
        srcDark: `img/${isSaas ? "acryl" : "datahub"}-logo-color-dark-horizontal.svg`,
        href: "https://datahub.com",
      },
      items: [
        {
          to: "/docs",
          activeBasePath: "docs",
          label: "Docs",
          position: "right",
        },
        {
          to: "/integrations",
          activeBasePath: "integrations",
          label: "Integrations",
          position: "right",
        },
        {
          type: "docsVersionDropdown",
          position: "left",
          dropdownActiveClassDisabled: true,
          dropdownItemsAfter: [
            {
              type: 'html',
              value: '<hr class="dropdown-separator" style="margin: 0.4rem;">',
            },
            {
              type: 'html',
              value: '<div class="dropdown__link"><b>Archived versions</b></div>',
            },
            {
              value: `
                     <a class="dropdown__link" href="https://docs-website-t9sv4w3gr-acryldata.vercel.app/docs/features">1.0.0
                     <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                     </a>
                     `,
              type: "html",
            },
            {
              value: `
                     <a class="dropdown__link" href="https://docs-website-t9sv4w3gr-acryldata.vercel.app/docs/0.15.0/features">0.15.0
                     <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                     </a>
                     `,
              type: "html",
            },
            {
              value: `
                     <a class="dropdown__link" href="https://docs-website-8jkm4uler-acryldata.vercel.app/docs/0.14.1/features">0.14.1
                     <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                     </a>
                     `,
              type: "html",
            },
            {
              value: `
                     <a class="dropdown__link" href="https://docs-website-eue2qafvn-acryldata.vercel.app/docs/features">0.14.0
                     <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                     </a>
                     `,
              type: "html",
            },
            {
              value: `
                   <a class="dropdown__link" href="https://docs-website-psat3nzgi-acryldata.vercel.app/docs/features">0.13.1
                   <svg width="12" height="12" aria-hidden="true" viewBox="0 0 24 24"><path fill="currentColor" d="M21 13v10h-21v-19h12v2h-10v15h17v-8h2zm3-12h-10.988l4.035 4-6.977 7.07 2.828 2.828 6.977-7.07 4.125 4.172v-11z"></path></svg>
                   </a>
                   `,
              type: "html",
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
        {
          href: "https://datahub.com/slack?utm_source=docs&utm_medium=header&utm_campaign=docs_header",
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
          href: "https://github.com/datahub-project/datahub",
          html: `
            <style>
              .github-logo:hover {
                opacity: 0.8;
              }
            </style>
            <img class='github-logo' src='https://upload.wikimedia.org/wikipedia/commons/9/91/Octicons-mark-github.svg', alt='slack', height='20px' style='margin: 10px 0 0 0;'/>
          `,
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
              href: "https://datahub.com/slack",
            },
            {
              label: "YouTube",
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
            },
            {
              label: "Blog",
              href: "https://medium.com/datahub-project",
            },
            {
              label: "Town Halls",
              to: "docs/townhalls",
            },
            {
              label: "Customer Stories",
              href: "https://datahub.com/resources/?2004611554=dh-stories",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "Demo",
              to: "https://demo.datahub.com/",
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
      // This is the "Search API Key" in Algolia, which means that it is ok to be public.
      apiKey: "2adf840a044a5ecbf7bdaac88cbf9ee5",
      appId: "RK0UG797F3",
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
              label: "1.3.0",
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
          ],
        },
        gtag: {
          trackingID: "G-2G54RXWD4D",
        },
        pages: {
          path: "src/pages",
          mdxPageComponent: "@theme/MDXPage",
        },
        googleTagManager: {
          containerId: 'GTM-5M8T9HNN',
        },
        gtag: {
          trackingID: "G-H5QDNJNMHY",
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
        redirects: [
          {
            from: '/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor',
            to: '/docs/managed-datahub/remote-executor/about',
          },
        ],
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
