import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import clsx from 'clsx';
import { useColorMode } from '@docusaurus/theme-common';
import SearchBar from '@theme/SearchBar';
import ColorModeToggle from '@theme/ColorModeToggle';
import styles from './styles.module.scss';
import DocsVersionDropdownNavbarItem from '@theme/NavbarItem/DocsVersionDropdownNavbarItem';

function SecondNavbarContent() {
  const { colorMode, setColorMode } = useColorMode();
  const location = useLocation();
  const isDocsPath = location.pathname.startsWith('/docs');
  if (!isDocsPath) {
    return null;
  }

  return (
    <div className={clsx(styles.secondNavbar, colorMode === 'dark' && styles.darkMode)}>
      <div className={styles.container}>
        <div className={styles.versionDropdown}>
          <DocsVersionDropdownNavbarItem
            docsPluginId="default"
            dropdownItemsBefore={[]}
            dropdownItemsAfter={[]}
            dropdownActiveClassDisabled={false}
            mobile={false}
          />
        </div>
        <div className={clsx(styles.navbarItemsRight)}>
          <div className={styles.colorModeToggle}>
            <ColorModeToggle
              className="clean-btn toggleButton_node_modules-@docusaurus-theme-classic-lib-theme-ColorModeToggle-styles-module"
              title={`Switch between dark and light mode (currently ${colorMode} mode)`}
              aria-label={`Switch between dark and light mode (currently ${colorMode} mode)`}
              aria-live="polite"
              onChange={() => setColorMode(colorMode === 'dark' ? 'light' : 'dark')}
            />
          </div>
          <div className={styles.searchBox}>
            <SearchBar />
          </div>
        </div>
      </div>
    </div>
  );
}

function SecondNavbar() {
  return (
      <SecondNavbarContent />
  );
}

export default SecondNavbar;