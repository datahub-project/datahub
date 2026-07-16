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
  docsPluginId,
  dropdownActiveClassDisabled,
  dropdownItemsBefore,
  dropdownItemsAfter,
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


  const items = [...dropdownItemsBefore, ...versionLinks, ...dropdownItemsAfter];
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