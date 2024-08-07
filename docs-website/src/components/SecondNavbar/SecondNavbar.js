import React from 'react';
import { useThemeConfig } from '@docusaurus/theme-common';
import { Link, useLocation } from 'react-router-dom';
import clsx from 'clsx';
import { useColorMode } from '@docusaurus/theme-common';
import SearchBar from '@theme/SearchBar';
import ColorModeToggle from '@theme/ColorModeToggle'; 
import styles from './styles.module.scss';

function SecondNavbar() {
  const { colorMode, setColorMode } = useColorMode();
  const { versions } = useThemeConfig();
  const location = useLocation(); 

  return (
    <div className={clsx(styles.secondNavbar, colorMode === 'dark' && styles.darkMode)}>
      <div className={styles.container}>
        {/* Conditionally rendered content under /docs */}
        {location.pathname.startsWith('/docs') && (
          <>
            {/* Core / Cloud Switch */}
            <div className={styles.coreCloudSwitch}>
              <Link
                className={clsx(styles.docsSwitchButton, location.pathname.includes('/docs/features') && styles.activeButton)}
                to="/docs/features"
              >
                DataHub Core
              </Link>
              <Link
                className={clsx(styles.docsSwitchButton, location.pathname.includes('/docs/managed-datahub/welcome-acryl') && styles.activeButton)}
                to="/docs/managed-datahub/welcome-acryl"
              >
                DataHub Cloud
              </Link>
            </div>

            {/* Light/Dark mode toggle */}
            <div className={styles.colorModeToggle}>
              <ColorModeToggle
                className="clean-btn toggleButton_node_modules-@docusaurus-theme-classic-lib-theme-ColorModeToggle-styles-module"
                title={`Switch between dark and light mode (currently ${colorMode} mode)`}
                aria-label={`Switch between dark and light mode (currently ${colorMode} mode)`}
                aria-live="polite"
                onChange={() => setColorMode(colorMode === 'dark' ? 'light' : 'dark')}
              />
            </div>
          </>
        )}

        {/* Search bar, always visible */}
        <div className={styles.searchBox}>
          <SearchBar />
        </div>
      </div>
    </div>
  );
}

export default SecondNavbar;
