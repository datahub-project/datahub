/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* eslint-disable jsx-a11y/no-autofocus */

import React, { useEffect, useState, useReducer, useRef } from "react";

import algoliaSearch from "algoliasearch/lite";
import algoliaSearchHelper from "algoliasearch-helper";
import clsx from "clsx";

import Link from "@docusaurus/Link";
import ExecutionEnvironment from "@docusaurus/ExecutionEnvironment";
import { usePluralForm, useEvent } from "@docusaurus/theme-common";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useAllDocsData } from "@docusaurus/plugin-content-docs/client";
import { useSearchPage } from "@docusaurus/theme-common/internal";
import Translate, { translate } from "@docusaurus/Translate";
import styles from "./search.module.scss";

// Very simple pluralization: probably good enough for now
function useDocumentsFoundPlural() {
  const { selectMessage } = usePluralForm();
  return (count) =>
    selectMessage(
      count,
      translate(
        {
          id: "theme.SearchPage.documentsFound.plurals",
          description:
            'Pluralized label for "{count} documents found". Use as much plural forms (separated by "|") as your language support (see https://www.unicode.org/cldr/cldr-aux/charts/34/supplemental/language_plural_rules.html)',
          message: "One document found|{count} documents found",
        },
        { count }
      )
    );
}

function useDocsSearchVersionsHelpers() {
  const allDocsData = useAllDocsData();

  // State of the version select menus / algolia facet filters
  // docsPluginId -> versionName map
  const [searchVersions, setSearchVersions] = useState(() => {
    return Object.entries(allDocsData).reduce((acc, [pluginId, pluginData]) => {
      return { ...acc, [pluginId]: pluginData.versions[0].name };
    }, {});
  });

  // Set the value of a single select menu
  const setSearchVersion = (pluginId, searchVersion) => setSearchVersions((s) => ({ ...s, [pluginId]: searchVersion }));

  const versioningEnabled = Object.values(allDocsData).some((docsData) => docsData.versions.length > 1);

  return {
    allDocsData,
    versioningEnabled,
    searchVersions,
    setSearchVersion,
  };
}

// We want to display one select per versioned docs plugin instance
const SearchVersionSelectList = ({ docsSearchVersionsHelpers }) => {
  const versionedPluginEntries = Object.entries(docsSearchVersionsHelpers.allDocsData)
    // Do not show a version select for unversioned docs plugin instances
    .filter(([, docsData]) => docsData.versions.length > 1);

  return (
    <div className={clsx("col", "col--3", "padding-left--none", styles.searchVersionColumn)}>
      {versionedPluginEntries.map(([pluginId, docsData]) => {
        const labelPrefix = versionedPluginEntries.length > 1 ? `${pluginId}: ` : "";
        return (
          <select
            key={pluginId}
            onChange={(e) => docsSearchVersionsHelpers.setSearchVersion(pluginId, e.target.value)}
            defaultValue={docsSearchVersionsHelpers.searchVersions[pluginId]}
            className={styles.searchVersionInput}
          >
            {docsData.versions.map((version, i) => (
              <option key={i} label={`${labelPrefix}${version.label}`} value={version.name} />
            ))}
          </select>
        );
      })}
    </div>
  );
};

function SearchBar() {
  const {
    siteConfig: {
      themeConfig: {
        algolia: { appId, apiKey, indexName },
      },
    },
    i18n: { currentLocale },
  } = useDocusaurusContext();
  const documentsFoundPlural = useDocumentsFoundPlural();

  const docsSearchVersionsHelpers = useDocsSearchVersionsHelpers();
  const { searchQuery, setSearchQuery } = useSearchPage()
  const initialSearchResultState = {
    items: [],
    query: null,
    totalResults: null,
    totalPages: null,
    lastPage: null,
    hasMore: null,
    loading: null,
  };
  const [searchResultState, searchResultStateDispatcher] = useReducer((prevState, { type, value: state }) => {
    switch (type) {
      case "reset": {
        return initialSearchResultState;
      }
      case "loading": {
        return { ...prevState, loading: true };
      }
      case "update": {
        if (searchQuery !== state.query) {
          return prevState;
        }

        return {
          ...state,
          items: state.lastPage === 0 ? state.items : prevState.items.concat(state.items),
        };
      }
      case "advance": {
        const hasMore = prevState.totalPages > prevState.lastPage + 1;

        return {
          ...prevState,
          lastPage: hasMore ? prevState.lastPage + 1 : prevState.lastPage,
          hasMore,
        };
      }
      default:
        return prevState;
    }
  }, initialSearchResultState);

  const algoliaClient = algoliaSearch(appId, apiKey);
  const algoliaHelper = algoliaSearchHelper(algoliaClient, indexName, {
    hitsPerPage: 15,
    advancedSyntax: true,
    disjunctiveFacets: ["language", "docusaurus_tag"],
  });

  algoliaHelper.on("result", ({ results: { query, hits, page, nbHits, nbPages } }) => {
    if (query === "" || !(hits instanceof Array)) {
      searchResultStateDispatcher({ type: "reset" });
      return;
    }

    const sanitizeValue = (value) => {
      return value.replace(/algolia-docsearch-suggestion--highlight/g, "search-result-match");
    };

    const items = hits.map(({ url, _highlightResult: { hierarchy }, _snippetResult: snippet = {} }) => {
      const { pathname, hash } = new URL(url);
      const titles = Object.keys(hierarchy).map((key) => {
        return sanitizeValue(hierarchy[key].value);
      });

      return {
        title: titles.pop(),
        url: pathname + hash,
        summary: snippet.content ? `${sanitizeValue(snippet.content.value)}...` : "",
        breadcrumbs: titles,
      };
    });

    searchResultStateDispatcher({
      type: "update",
      value: {
        items,
        query,
        totalResults: nbHits,
        totalPages: nbPages,
        lastPage: page,
        hasMore: nbPages > page + 1,
        loading: false,
      },
    });
  });

  const [loaderRef, setLoaderRef] = useState(null);
  const prevY = useRef(0);
  const observer = useRef(
    ExecutionEnvironment.canUseDOM &&
      new IntersectionObserver(
        (entries) => {
          const {
            isIntersecting,
            boundingClientRect: { y: currentY },
          } = entries[0];

          if (isIntersecting && prevY.current > currentY) {
            searchResultStateDispatcher({ type: "advance" });
          }

          prevY.current = currentY;
        },
        { threshold: 1 }
      )
  );

  const getTitle = () =>
    searchQuery
      ? translate(
          {
            id: "theme.SearchPage.existingResultsTitle",
            message: 'Search results for "{query}"',
            description: "The search page title for non-empty query",
          },
          {
            query: searchQuery,
          }
        )
      : translate({
          id: "theme.SearchPage.emptyResultsTitle",
          message: "Search the documentation",
          description: "The search page title for empty query",
        });

  const makeSearch = useEvent((page = 0) => {
    algoliaHelper.addDisjunctiveFacetRefinement("docusaurus_tag", "default");
    algoliaHelper.addDisjunctiveFacetRefinement("language", currentLocale);

    Object.entries(docsSearchVersionsHelpers.searchVersions).forEach(([pluginId, searchVersion]) => {
      algoliaHelper.addDisjunctiveFacetRefinement("docusaurus_tag", `docs-${pluginId}-${searchVersion}`);
    });

    algoliaHelper.setQuery(searchQuery).setPage(page).search();
  });

  useEffect(() => {
    if (!loaderRef) {
      return undefined;
    }
    const currentObserver = observer.current;

    currentObserver.observe(loaderRef);
    return () => currentObserver.unobserve(loaderRef);
  }, [loaderRef]);

  useEffect(() => {
    searchResultStateDispatcher({ type: "reset" });

    if (searchQuery) {
      searchResultStateDispatcher({ type: "loading" });

      setTimeout(() => {
        makeSearch();
      }, 300);
    }
  }, [searchQuery, docsSearchVersionsHelpers.searchVersions, makeSearch]);

  useEffect(() => {
    if (!searchResultState.lastPage || searchResultState.lastPage === 0) {
      return;
    }

    makeSearch(searchResultState.lastPage);
  }, [makeSearch, searchResultState.lastPage]);

  return (
    <div className="DocSearch row">
      <div className="col col--offset-3 col--6">
        <form onSubmit={(e) => e.preventDefault()} className={styles.searchForm}>
          <input
            type="search"
            name="q"
            className={styles.searchQueryInput}
            placeholder={translate({
              id: "theme.SearchPage.inputPlaceholder",
              message: "Search the docs",
              description: "The placeholder for search page input",
            })}
            aria-label={translate({
              id: "theme.SearchPage.inputLabel",
              message: "Search",
              description: "The ARIA label for search page input",
            })}
            onChange={(e) => setSearchQuery(e.target.value)}
            value={searchQuery}
            autoComplete="off"
            autoFocus
          />
          <svg width="20" height="20" className={clsx("DocSearch-Search-Icon", styles.searchIcon)} viewBox="0 0 20 20">
            <path
              d="M14.386 14.386l4.0877 4.0877-4.0877-4.0877c-2.9418 2.9419-7.7115 2.9419-10.6533 0-2.9419-2.9418-2.9419-7.7115 0-10.6533 2.9418-2.9419 7.7115-2.9419 10.6533 0 2.9419 2.9418 2.9419 7.7115 0 10.6533z"
              stroke="currentColor"
              fill="none"
              fillRule="evenodd"
              strokeLinecap="round"
              strokeLinejoin="round"
            ></path>
          </svg>

          {docsSearchVersionsHelpers.versioningEnabled && <SearchVersionSelectList docsSearchVersionsHelpers={docsSearchVersionsHelpers} />}
        </form>

        <div className={styles.searchResultsColumn}>{!!searchResultState.totalResults && documentsFoundPlural(searchResultState.totalResults)}</div>

        {searchResultState.items.length > 0 ? (
          <main>
            {searchResultState.items.map(({ title, url, summary, breadcrumbs }, i) => (
              <article key={i} className={styles.searchResultItem}>
                <h3 className={styles.searchResultItemHeading}>
                  <Link to={url} dangerouslySetInnerHTML={{ __html: title }} />
                </h3>

                {breadcrumbs.length > 0 && (
                  <nav aria-label="breadcrumbs">
                    <ul className={clsx("breadcrumbs", styles.searchResultItemPath)}>
                      {breadcrumbs.map((html, index) => (
                        <li
                          key={index}
                          className="breadcrumbs__item"
                          // Developer provided the HTML, so assume it's safe.
                          // eslint-disable-next-line react/no-danger
                          dangerouslySetInnerHTML={{ __html: html }}
                        />
                      ))}
                    </ul>
                  </nav>
                )}

                {summary && (
                  <p
                    className={styles.searchResultItemSummary}
                    // Developer provided the HTML, so assume it's safe.
                    // eslint-disable-next-line react/no-danger
                    dangerouslySetInnerHTML={{ __html: summary }}
                  />
                )}
              </article>
            ))}
          </main>
        ) : (
          [
            searchQuery && !searchResultState.loading && (
              <p key="no-results">
                <Translate id="theme.SearchPage.noResultsText" description="The paragraph for empty search result">
                  No results were found
                </Translate>
              </p>
            ),
            !!searchResultState.loading && <div key="spinner" className={styles.loadingSpinner} />,
          ]
        )}

        {searchResultState.hasMore && (
          <div className={styles.loader} ref={setLoaderRef}>
            <Translate id="theme.SearchPage.fetchingNewResults" description="The paragraph for fetching new search results">
              Fetching new results...
            </Translate>
          </div>
        )}
      </div>
    </div>
  );
}

export default SearchBar;
