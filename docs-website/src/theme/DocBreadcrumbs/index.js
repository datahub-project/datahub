// Swizzled from @docusaurus/theme-classic v2.4.1 to fix Schema.org BreadcrumbList
// validation errors flagged by Google Search Console.
//
// The upstream component (see https://github.com/facebook/docusaurus/issues/7241)
// emits `<meta itemProp="position">` on every breadcrumb item but only adds the
// surrounding `itemScope itemProp="itemListElement" itemType=".../ListItem"`
// microdata when the item has an href. For non-link breadcrumb items (parent
// categories that aren't themselves linkable), this leaves an orphan `position`
// property that Google rejects with: "The property position is not recognized
// by Schema.org vocabulary."
//
// This swizzle wraps every breadcrumb item in proper ListItem microdata so the
// `position` property is always within a valid scope. Non-link items also get
// `itemProp="name"` so the ListItem has a name even without a URL.

import React from "react";
import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import {
  useSidebarBreadcrumbs,
  useHomePageRoute,
} from "@docusaurus/theme-common/internal";
import Link from "@docusaurus/Link";
import { translate } from "@docusaurus/Translate";
import HomeBreadcrumbItem from "@theme/DocBreadcrumbs/Items/Home";

import styles from "./styles.module.css";

function BreadcrumbsItemLink({ children, href, isLast }) {
  const className = "breadcrumbs__link";
  if (isLast) {
    // Last (active) breadcrumb: no link, but still carries the ListItem name.
    return (
      <span className={className} itemProp="name">
        {children}
      </span>
    );
  }
  if (href) {
    return (
      <Link className={className} href={href} itemProp="item">
        <span itemProp="name">{children}</span>
      </Link>
    );
  }
  // Intermediate non-link breadcrumb (parent category without its own page).
  // Provide `itemProp="name"` so the surrounding ListItem still has a name.
  return (
    <span className={className} itemProp="name">
      {children}
    </span>
  );
}

function BreadcrumbsItem({ children, active, index }) {
  return (
    <li
      itemScope
      itemProp="itemListElement"
      itemType="https://schema.org/ListItem"
      className={clsx("breadcrumbs__item", {
        "breadcrumbs__item--active": active,
      })}
    >
      {children}
      <meta itemProp="position" content={String(index + 1)} />
    </li>
  );
}

export default function DocBreadcrumbs() {
  const breadcrumbs = useSidebarBreadcrumbs();
  const homePageRoute = useHomePageRoute();

  if (!breadcrumbs) {
    return null;
  }

  return (
    <nav
      className={clsx(
        ThemeClassNames.docs.docBreadcrumbs,
        styles.breadcrumbsContainer,
      )}
      aria-label={translate({
        id: "theme.docs.breadcrumbs.navAriaLabel",
        message: "Breadcrumbs",
        description: "The ARIA label for the breadcrumbs",
      })}
    >
      <ul
        className="breadcrumbs"
        itemScope
        itemType="https://schema.org/BreadcrumbList"
      >
        {homePageRoute && <HomeBreadcrumbItem />}
        {breadcrumbs.map((item, idx) => {
          const isLast = idx === breadcrumbs.length - 1;
          return (
            <BreadcrumbsItem key={idx} active={isLast} index={idx}>
              <BreadcrumbsItemLink href={item.href} isLast={isLast}>
                {item.label}
              </BreadcrumbsItemLink>
            </BreadcrumbsItem>
          );
        })}
      </ul>
    </nav>
  );
}
