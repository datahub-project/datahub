import React from "react";
import { useVersions, useActiveDocContext } from "@docusaurus/plugin-content-docs/client";
import { useDocsPreferredVersion } from "@docusaurus/theme-common";
import { useDocsVersionCandidates } from "@docusaurus/theme-common/internal";
import { translate } from "@docusaurus/Translate";
import { useLocation } from "@docusaurus/router";
import DefaultNavbarItem from "@theme/NavbarItem/DefaultNavbarItem";
import DropdownNavbarItem from "@theme/NavbarItem/DropdownNavbarItem";

import styles from "./styles.module.scss";

const getVersionMainDoc = (version) => version.docs.find((doc) => doc.id === version.mainDocId);

export default function DocsVersionDropdownNavbarItem({
  mobile,
  docsPluginId = 'default',
  dropdownActiveClassDisabled = false,
  dropdownItemsBefore = [],
  dropdownItemsAfter = [],
  ...props
}) {
  const { search, hash } = useLocation();
  const activeDocContext = useActiveDocContext(docsPluginId);
  const versions = useVersions(docsPluginId);
  const { savePreferredVersionName } = useDocsPreferredVersion(docsPluginId);
  const versionLinks = versions.map((version) => {
    const versionDoc = activeDocContext.alternateDocVersions[version.name] ?? getVersionMainDoc(version);
    return {
      label: version.label,
      to: `${versionDoc.path}${search}${hash}`,
      isActive: () => version === activeDocContext.activeVersion,
      onClick: () => savePreferredVersionName(version.name),
    };
  });

  const archivedVersions = [
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
         <a class="dropdown__link" href="https://docs-website-eue2qafvn-acryldata.vercel.app//docs/features">0.14.0
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
  ];

  const items = [...dropdownItemsBefore, ...versionLinks, ...archivedVersions, ...dropdownItemsAfter];
  const dropdownVersion = useDocsVersionCandidates(docsPluginId)[0];
  const dropdownLabel =
    mobile && items.length > 1
      ? translate({
          id: "theme.navbar.mobileVersionsDropdown.label",
          message: "Versions",
          description: "The label for the navbar versions dropdown on mobile view",
        })
      : dropdownVersion.label;
  const dropdownTo = mobile && items.length > 1 ? undefined : getVersionMainDoc(dropdownVersion).path;

  if (items.length <= 1) {
    return (
      <DefaultNavbarItem
        {...props}
        mobile={mobile}
        label={dropdownLabel}
        to={dropdownTo}
        isActive={dropdownActiveClassDisabled ? () => false : undefined}
      />
    );
  }
  return (
    <DropdownNavbarItem
      {...props}
      className={styles.versionNavItem}
      mobile={mobile}
      label={dropdownLabel}
      to={false}
      items={items}
      isActive={dropdownActiveClassDisabled ? () => false : undefined}
    />
  );
}