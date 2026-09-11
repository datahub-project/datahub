import { createGlobalStyle } from 'styled-components';

import { Theme } from '@conf/theme/types';

/**
 * Global CSS overrides that make Ant Design components respect the active theme's
 * semantic color tokens. These are injected via styled-components' createGlobalStyle,
 * which receives the current theme from ThemeProvider.
 *
 * This bridges the gap between the build-time Ant Less variables and our
 * runtime theme switching (light ↔ dark).
 */
const GlobalThemeStyles = createGlobalStyle<{ theme: Theme }>`
    /* ── Base ─────────────────────────────────────────────── */
    :root {
        --theme-bgSurface: ${(props) => props.theme.colors.bgSurface};
        --theme-bgSelected: ${(props) => props.theme.colors.bgSelected};
        --theme-bgSelectedSubtle: ${(props) => props.theme.colors.bgSelectedSubtle};
        --theme-bgHighlight: ${(props) => props.theme.colors.bgHighlight};
        --theme-shadowFocus: ${(props) => props.theme.colors.shadowFocus};
        --theme-shadowFocusBrand: ${(props) => props.theme.colors.shadowFocusBrand};
        --theme-overlayLight: ${(props) => props.theme.colors.overlayLight};
        --theme-overlayMedium: ${(props) => props.theme.colors.overlayMedium};
        --theme-overlayHeavy: ${(props) => props.theme.colors.overlayHeavy};
        --theme-icon: ${(props) => props.theme.colors.icon};
        --theme-iconBrand: ${(props) => props.theme.colors.iconBrand};
        --theme-textOnFillBrand: ${(props) => props.theme.colors.textOnFillBrand};
        --datahub-logo-fg: ${(props) => props.theme.colors.text};
        --datahub-logo-pill-bg: ${(props) => props.theme.colors.bgSurfaceDarker};
        --datahub-logo-pill-fg: ${(props) => props.theme.colors.textSecondary};
    }

    body {
        background-color: ${(props) => props.theme.colors.bgSurfaceNewNav};
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Ant Layout ───────────────────────────────────────── */
    .ant-layout {
        background-color: transparent;
        color: ${(props) => props.theme.colors.text};
    }
    .ant-layout-header,
    .ant-layout-footer {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-layout-sider-trigger {
        background-color: ${(props) => props.theme.colors.bgSurfaceDarker};
        color: ${(props) => props.theme.colors.textOnFillDefault};
    }

    /* ── Modals ───────────────────────────────────────────── */
    .ant-modal {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-modal-content {
        background-color: ${(props) => props.theme.colors.bg} !important;
        color: ${(props) => props.theme.colors.text} !important;
        box-shadow: ${(props) => props.theme.colors.shadowLg};
    }
    .ant-modal-header {
        background-color: ${(props) => props.theme.colors.bg} !important;
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-modal-header .ant-modal-title {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-modal-footer {
        border-top-color: ${(props) => props.theme.colors.border};
    }
    .ant-modal-body {
        background-color: ${(props) => props.theme.colors.bg} !important;
    }
    .ant-modal-close-x {
        color: ${(props) => props.theme.colors.icon} !important;
    }
    .ant-modal-close:hover,
    .ant-modal-close:focus {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-modal-mask {
        background-color: ${(props) => props.theme.colors.overlayHeavy};
    }
    .ant-modal-confirm-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-modal-confirm-content {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-modal-confirm-confirm .ant-modal-confirm-body > .anticon {
        color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-modal-confirm-warning .ant-modal-confirm-body > .anticon {
        color: ${(props) => props.theme.colors.iconWarning};
    }
    .ant-modal-confirm-error .ant-modal-confirm-body > .anticon {
        color: ${(props) => props.theme.colors.iconError};
    }
    .ant-modal-confirm-success .ant-modal-confirm-body > .anticon {
        color: ${(props) => props.theme.colors.iconSuccess};
    }

    /* ── Reactour (Product Tour) ────────────────────────── */
    .reactour__helper {
        background-color: ${(props) => props.theme.colors.bg} !important;
        color: ${(props) => props.theme.colors.text} !important;
    }
    .reactour__helper h1,
    .reactour__helper h2,
    .reactour__helper h3,
    .reactour__helper h4,
    .reactour__helper h5,
    .reactour__helper h6 {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .reactour__helper p,
    .reactour__helper span,
    .reactour__helper div.ant-typography {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .reactour__helper a {
        color: ${(props) => props.theme.colors.hyperlinks} !important;
    }
    .reactour__close {
        color: ${(props) => props.theme.colors.icon} !important;
    }

    /* ── Dropdowns ────────────────────────────────────────── */
    .ant-dropdown {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-dropdown-menu {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-dropdown-menu .ant-dropdown-menu-item,
    .ant-dropdown-menu .ant-dropdown-menu-submenu-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-dropdown-menu .ant-dropdown-menu-item:hover,
    .ant-dropdown-menu .ant-dropdown-menu-submenu-title:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-dropdown-menu .ant-dropdown-menu-item-danger,
    .ant-dropdown-menu .ant-dropdown-menu-item-danger .anticon {
        color: ${(props) => props.theme.colors.textError} !important;
    }
    .ant-dropdown-menu .ant-dropdown-menu-item-danger:hover {
        background-color: ${(props) => props.theme.colors.bgSurfaceError} !important;
    }
    .ant-dropdown-menu .ant-dropdown-menu-item-disabled,
    .ant-dropdown-menu .ant-dropdown-menu-submenu-disabled {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-dropdown-menu-item-divider {
        background-color: ${(props) => props.theme.colors.border};
    }
    .ant-dropdown-arrow {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
    .ant-dropdown-menu-submenu-popup {
        .ant-dropdown-menu,
        .ant-dropdown-menu-sub {
            background-color: ${(props) => props.theme.colors.bg} !important;
            box-shadow: ${(props) => props.theme.colors.shadowMd} !important;
        }
        .ant-dropdown-menu-item,
        .ant-dropdown-menu-submenu-title {
            color: ${(props) => props.theme.colors.text} !important;
        }
        .ant-dropdown-menu-item:hover,
        .ant-dropdown-menu-submenu-title:hover {
            background-color: ${(props) => props.theme.colors.bgHover} !important;
        }
    }
    .ant-select-dropdown {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-select-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-select-item-option-active:not(.ant-select-item-option-disabled) {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-select-item-option-selected:not(.ant-select-item-option-disabled) {
        background-color: ${(props) => props.theme.colors.bgSelected};
        color: ${(props) => props.theme.colors.textSelected};
    }
    .ant-select-dropdown-empty {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-select-auto-complete {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Inputs ───────────────────────────────────────────── */
    .ant-input {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-input:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-input:focus,
    .ant-input-focused {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
    .ant-input::placeholder {
        color: ${(props) => props.theme.colors.textPlaceholder};
    }
    .ant-input[disabled] {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-select:not(.ant-select-customize-input) .ant-select-selector {
        background-color: ${(props) => props.theme.colors.bgInput};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.borderInput};
        box-shadow: ${(props) => props.theme.colors.shadowXs};
    }
    .ant-select-selection-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-select-selection-placeholder {
        color: ${(props) => props.theme.colors.textPlaceholder};
    }
    .ant-select-arrow,
    .ant-select-clear {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-select-clear {
        background-color: ${(props) => props.theme.colors.bgInput};
    }
    .ant-select-clear:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-select-disabled.ant-select:not(.ant-select-customize-input) .ant-select-selector {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-select-disabled .ant-select-arrow,
    .ant-select-disabled .ant-select-selection-item {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-select-single.ant-select-open .ant-select-selection-item {
        color: ${(props) => props.theme.colors.textPlaceholder};
    }
    .ant-select-multiple .ant-select-selection-item {
        background-color: ${(props) => props.theme.colors.bgSelectedSubtle};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-select-multiple .ant-select-selection-item-remove {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-select-multiple .ant-select-selection-item-remove:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-select:not(.ant-select-disabled):hover .ant-select-selector,
    .ant-select-focused:not(.ant-select-disabled) .ant-select-selector {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
    }
    .ant-select-focused:not(.ant-select-disabled) .ant-select-selector {
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
    .ant-select-status-error:not(.ant-select-disabled) .ant-select-selector {
        border-color: ${(props) => props.theme.colors.borderError};
    }
    .ant-select-status-warning:not(.ant-select-disabled) .ant-select-selector {
        border-color: ${(props) => props.theme.colors.borderWarning};
    }
    .ant-input-affix-wrapper {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
        box-shadow: ${(props) => props.theme.colors.shadowXs};
    }
    .ant-input-affix-wrapper:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-input-affix-wrapper-focused {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
    .ant-input,
    .ant-input-affix-wrapper .ant-input {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
    }
    .ant-input-affix-wrapper .ant-input {
        background-color: transparent;
    }
    .ant-input-prefix,
    .ant-input-suffix,
    .ant-input-clear-icon {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-input-clear-icon:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-input-status-error:not(.ant-input-disabled),
    .ant-input-affix-wrapper-status-error:not(.ant-input-affix-wrapper-disabled) {
        border-color: ${(props) => props.theme.colors.borderError};
    }
    .ant-input-status-warning:not(.ant-input-disabled),
    .ant-input-affix-wrapper-status-warning:not(.ant-input-affix-wrapper-disabled) {
        border-color: ${(props) => props.theme.colors.borderWarning};
    }
    .ant-input-status-error:not(.ant-input-disabled):focus,
    .ant-input-affix-wrapper-status-error.ant-input-affix-wrapper-focused,
    .ant-input-status-warning:not(.ant-input-disabled):focus,
    .ant-input-affix-wrapper-status-warning.ant-input-affix-wrapper-focused {
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }

    /* ── Tables ───────────────────────────────────────────── */
    .ant-table {
        background-color: ${(props) => props.theme.colors.bg} !important;
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-table-thead > tr > th {
        background-color: ${(props) => props.theme.colors.bgSurface} !important;
        color: ${(props) => props.theme.colors.text} !important;
        border-bottom-color: ${(props) => props.theme.colors.border} !important;
    }
    .ant-table-tbody > tr > td {
        border-bottom-color: ${(props) => props.theme.colors.border} !important;
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-table-tbody > tr:hover > td {
        background-color: ${(props) => props.theme.colors.bgHover} !important;
    }
    .ant-table-placeholder {
        background-color: ${(props) => props.theme.colors.bg} !important;
    }
    .ant-table-cell {
        background-color: inherit !important;
    }
    .ant-table-tbody > tr.ant-table-row-selected > td {
        background-color: ${(props) => props.theme.colors.bgSelectedSubtle} !important;
    }
    .ant-table-tbody > tr.ant-table-row-selected:hover > td {
        background-color: ${(props) => props.theme.colors.bgSelected} !important;
    }
    .ant-table-footer,
    .ant-table-summary {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-table-filter-trigger,
    .ant-table-column-sorter {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-table-filter-trigger:hover,
    .ant-table-filter-trigger.active,
    .ant-table-column-sorter-up.active,
    .ant-table-column-sorter-down.active {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-table-filter-dropdown {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-table-row-expand-icon {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-table-expanded-row > td,
    .ant-table-expanded-row:hover > td {
        background-color: ${(props) => props.theme.colors.bgSurface} !important;
    }
    .ant-table-sticky-holder,
    .ant-table-sticky-scroll {
        background-color: ${(props) => props.theme.colors.bg};
    }

    /* ── Cards ────────────────────────────────────────────── */
    .ant-card {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-bordered {
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-hoverable:hover,
    .ant-card-grid:hover {
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-card-head {
        color: ${(props) => props.theme.colors.text};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-actions {
        background-color: ${(props) => props.theme.colors.bgSurface};
        border-top-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-actions > li {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-card-actions > li:not(:last-child) {
        border-right-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-meta-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-card-meta-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-card-extra {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-card-type-inner .ant-card-head {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }

    /* ── Popover / Tooltip ────────────────────────────────── */
    .ant-popover {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-popover-inner {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-popover-inner-content {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-popover-title {
        color: ${(props) => props.theme.colors.text};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-popover-arrow-content {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
    .ant-popover-message {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-popover-message > .anticon {
        color: ${(props) => props.theme.colors.iconWarning};
    }

    /* ── Tabs ─────────────────────────────────────────────── */
    .ant-tabs {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-tabs-tab {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-tabs-tab:hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-tabs-tab.ant-tabs-tab-disabled,
    .ant-tabs-tab.ant-tabs-tab-disabled .ant-tabs-tab-btn {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-tabs-nav::before {
        border-bottom-color: ${(props) => props.theme.colors.border} !important;
    }
    .ant-tabs-ink-bar {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
    }
    .ant-tabs-dropdown-menu {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-tabs-dropdown-menu-item:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-tabs-card > .ant-tabs-nav .ant-tabs-tab,
    .ant-tabs-nav-add {
        background-color: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-tabs-nav-wrap::before,
    .ant-tabs-nav-wrap::after {
        box-shadow: ${(props) => props.theme.colors.shadowInset};
    }

    /* ── List ─────────────────────────────────────────────── */
    .ant-list {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-list-item {
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-list-item-meta-title,
    .ant-list-item-meta-title > a {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-list-item-meta-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-list-bordered {
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-list-item-action-split {
        background-color: ${(props) => props.theme.colors.border};
    }

    /* ── Menu ─────────────────────────────────────────────── */
    .ant-menu {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-menu-item:hover,
    .ant-menu-item-active {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-menu-item-selected {
        background-color: ${(props) => props.theme.colors.bgSelected};
        color: ${(props) => props.theme.colors.textSelected};
    }
    .ant-menu-submenu-title,
    .ant-menu-item a {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-menu-item-disabled,
    .ant-menu-submenu-disabled,
    .ant-menu-item-disabled a {
        color: ${(props) => props.theme.colors.textDisabled} !important;
    }
    .ant-menu-item-divider {
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-menu-item-group-title {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    .ant-menu-submenu-selected,
    .ant-menu-submenu-selected > .ant-menu-submenu-title {
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-menu-submenu-arrow {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-menu-item-danger,
    .ant-menu-item-danger a {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-menu-item-danger:hover,
    .ant-menu-item-danger:active {
        background-color: ${(props) => props.theme.colors.bgSurfaceError};
    }
    .ant-menu-sub.ant-menu-inline {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }

    /* ── Divider ──────────────────────────────────────────── */
    .ant-divider {
        border-top-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-divider-dashed {
        border-color: ${(props) => props.theme.colors.border};
    }

    /* ── Typography ───────────────────────────────────────── */
    /*
     * antd compiles its typography palette from LESS at build time, so those colors can never
     * follow the runtime theme. The blocks below mirror every color-bearing selector in
     * antd/lib/typography/style/index.css and re-point it at a semantic token.
     *
     * The body prefix is load-bearing: antd targets headings as ".ant-typography h1" (0,1,1),
     * which outranks a bare "h1" (0,0,1). Dropping the prefix hands headings back to antd's
     * compiled near-black heading color, which stays dark in dark mode.
     */
    h1, h2, h3, h4, h5, h6 {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-typography {
        color: ${(props) => props.theme.colors.text};
    }
    body h1.ant-typography,
    body h2.ant-typography,
    body h3.ant-typography,
    body h4.ant-typography,
    body h5.ant-typography,
    body h6.ant-typography,
    body .ant-typography h1,
    body .ant-typography h2,
    body .ant-typography h3,
    body .ant-typography h4,
    body .ant-typography h5,
    body .ant-typography h6,
    body div.ant-typography-h1,
    body div.ant-typography-h2,
    body div.ant-typography-h3,
    body div.ant-typography-h4,
    body div.ant-typography-h5,
    body div.ant-typography-h1 > textarea,
    body div.ant-typography-h2 > textarea,
    body div.ant-typography-h3 > textarea,
    body div.ant-typography-h4 > textarea,
    body div.ant-typography-h5 > textarea {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-typography.ant-typography-secondary {
        color: ${(props) => props.theme.colors.textSecondary} !important;
    }
    .ant-typography.ant-typography-success {
        color: ${(props) => props.theme.colors.textSuccess} !important;
    }
    .ant-typography.ant-typography-danger {
        color: ${(props) => props.theme.colors.textError} !important;
    }
    .ant-typography.ant-typography-warning {
        color: ${(props) => props.theme.colors.textWarning} !important;
    }
    .ant-typography.ant-typography-disabled {
        color: ${(props) => props.theme.colors.textDisabled} !important;
    }
    body a.ant-typography,
    body .ant-typography a {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
    body a.ant-typography:hover,
    body a.ant-typography:focus-visible,
    body .ant-typography a:hover,
    body .ant-typography a:focus-visible {
        color: ${(props) => props.theme.colors.textHover};
    }
    body a.ant-typography:active,
    body .ant-typography a:active {
        color: ${(props) => props.theme.colors.textActive};
    }
    body a.ant-typography[disabled],
    body a.ant-typography.ant-typography-disabled,
    body .ant-typography a[disabled],
    body .ant-typography a.ant-typography-disabled {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    body .ant-typography-expand,
    body .ant-typography-edit,
    body .ant-typography-copy {
        color: ${(props) => props.theme.colors.icon};
    }
    body .ant-typography-expand:hover,
    body .ant-typography-expand:focus-visible,
    body .ant-typography-edit:hover,
    body .ant-typography-edit:focus-visible,
    body .ant-typography-copy:hover,
    body .ant-typography-copy:focus-visible {
        color: ${(props) => props.theme.colors.iconHover};
    }
    body .ant-typography-expand:active,
    body .ant-typography-edit:active,
    body .ant-typography-copy:active {
        color: ${(props) => props.theme.colors.textActive};
    }
    body .ant-typography-copy-success,
    body .ant-typography-copy-success:hover,
    body .ant-typography-copy-success:focus {
        color: ${(props) => props.theme.colors.textSuccess};
    }
    body .ant-typography-edit-content-confirm {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    body .ant-typography code,
    body .ant-typography kbd {
        color: ${(props) => props.theme.colors.text};
        background: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
    }
    body .ant-typography pre {
        background: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
    }
    body .ant-typography blockquote {
        border-left-color: ${(props) => props.theme.colors.border};
    }
    body .ant-typography mark {
        color: ${(props) => props.theme.colors.text};
        background-color: ${(props) => props.theme.colors.bgHighlight};
    }

    /* ── Tag ──────────────────────────────────────────────── */
    .ant-tag {
        background-color: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.textSecondary};
        .ant-tag-close-icon {
            color: ${(props) => props.theme.colors.icon};
        }
    }
    .ant-tag a,
    .ant-tag a:hover {
        color: inherit;
    }
    .ant-tag-checkable:not(.ant-tag-checkable-checked):hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-tag-checkable-checked,
    .ant-tag-checkable:active {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }
    .ant-tag-success,
    .ant-tag-green {
        background-color: ${(props) => props.theme.colors.tagsDeepGreenBg};
        border-color: ${(props) => props.theme.colors.tagsDeepGreenBorder};
        color: ${(props) => props.theme.colors.tagsDeepGreenText};
    }
    .ant-tag-processing,
    .ant-tag-blue {
        background-color: ${(props) => props.theme.colors.tagsTrueBlueBg};
        border-color: ${(props) => props.theme.colors.tagsTrueBlueBorder};
        color: ${(props) => props.theme.colors.tagsTrueBlueText};
    }
    .ant-tag-error,
    .ant-tag-red {
        background-color: ${(props) => props.theme.colors.bgSurfaceError};
        border-color: ${(props) => props.theme.colors.borderError};
        color: ${(props) => props.theme.colors.textOnSurfaceError};
    }
    .ant-tag-warning,
    .ant-tag-gold,
    .ant-tag-yellow {
        background-color: ${(props) => props.theme.colors.tagsTrueYellowBg};
        border-color: ${(props) => props.theme.colors.tagsTrueYellowBorder};
        color: ${(props) => props.theme.colors.tagsTrueYellowText};
    }
    .ant-tag-orange,
    .ant-tag-volcano {
        background-color: ${(props) => props.theme.colors.tagsTangerineBg};
        border-color: ${(props) => props.theme.colors.tagsTangerineBorder};
        color: ${(props) => props.theme.colors.tagsTangerineText};
    }
    .ant-tag-cyan {
        background-color: ${(props) => props.theme.colors.tagsCyanBg};
        border-color: ${(props) => props.theme.colors.tagsCyanBorder};
        color: ${(props) => props.theme.colors.tagsCyanText};
    }
    .ant-tag-lime {
        background-color: ${(props) => props.theme.colors.tagsOliveBg};
        border-color: ${(props) => props.theme.colors.tagsOliveBorder};
        color: ${(props) => props.theme.colors.tagsOliveText};
    }
    .ant-tag-geekblue {
        background-color: ${(props) => props.theme.colors.tagsCobaltBlueBg};
        border-color: ${(props) => props.theme.colors.tagsCobaltBlueBorder};
        color: ${(props) => props.theme.colors.tagsCobaltBlueText};
    }
    .ant-tag-purple {
        background-color: ${(props) => props.theme.colors.tagsLavenderBg};
        border-color: ${(props) => props.theme.colors.tagsLavenderBorder};
        color: ${(props) => props.theme.colors.tagsLavenderText};
    }
    .ant-tag-pink,
    .ant-tag-magenta {
        background-color: ${(props) => props.theme.colors.tagsCoralBg};
        border-color: ${(props) => props.theme.colors.tagsCoralBorder};
        color: ${(props) => props.theme.colors.tagsCoralText};
    }
    .ant-tag[class*='-inverse'] {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }

    /* ── Form ─────────────────────────────────────────────── */
    .ant-form,
    .ant-form-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-form-item-label > label {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-form-item-label > label.ant-form-item-required:not(.ant-form-item-required-mark-optional)::before,
    .ant-form-item-explain-error {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-form-item-extra,
    .ant-form-item-explain,
    .ant-form-text {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-form-item-explain-warning,
    .ant-form-item-has-warning .ant-form-item-split {
        color: ${(props) => props.theme.colors.textWarning};
    }
    .ant-form-item-has-error .ant-form-item-split {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-form-item-feedback-icon-success {
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-form-item-feedback-icon-error {
        color: ${(props) => props.theme.colors.iconError};
    }
    .ant-form-item-feedback-icon-warning {
        color: ${(props) => props.theme.colors.iconWarning};
    }
    .ant-form-item-feedback-icon-validating {
        color: ${(props) => props.theme.colors.iconBrand};
    }

    /* ── Checkbox ─────────────────────────────────────────── */
    .ant-checkbox,
    .ant-checkbox-wrapper,
    .ant-checkbox-group {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-checkbox-inner,
    .ant-checkbox-indeterminate .ant-checkbox-inner {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderCheckbox};
    }
    .ant-checkbox-wrapper:hover .ant-checkbox-inner,
    .ant-checkbox:hover .ant-checkbox-inner,
    .ant-checkbox-input:focus + .ant-checkbox-inner {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-checkbox-checked::after {
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-checkbox-checked .ant-checkbox-inner {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-checkbox-checked .ant-checkbox-inner::after {
        border-color: ${(props) => props.theme.colors.iconOnFillBrand};
    }
    .ant-checkbox-indeterminate .ant-checkbox-inner::after {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
    }
    .ant-checkbox-disabled .ant-checkbox-inner {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled} !important;
    }
    .ant-checkbox-disabled.ant-checkbox-checked .ant-checkbox-inner::after,
    .ant-checkbox-indeterminate.ant-checkbox-disabled .ant-checkbox-inner::after {
        border-color: ${(props) => props.theme.colors.iconDisabled};
    }
    .ant-checkbox-indeterminate.ant-checkbox-disabled .ant-checkbox-inner::after {
        background-color: ${(props) => props.theme.colors.iconDisabled};
    }
    .ant-checkbox-disabled + span {
        color: ${(props) => props.theme.colors.textDisabled};
    }

    /* ── Radio ────────────────────────────────────────────── */
    .ant-radio,
    .ant-radio-wrapper,
    .ant-radio-group {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-radio-inner {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.radioButtonBorder};
    }
    .ant-radio-wrapper:hover .ant-radio,
    .ant-radio:hover .ant-radio-inner,
    .ant-radio-input:focus + .ant-radio-inner {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-radio-checked .ant-radio-inner {
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-radio-inner::after {
        background-color: ${(props) => props.theme.colors.radioButtonDotFill};
    }
    .ant-radio-checked::after {
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-radio-disabled .ant-radio-inner {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled} !important;
    }
    .ant-radio-disabled .ant-radio-inner::after {
        background-color: ${(props) => props.theme.colors.radioButtonDotDisabled};
    }
    .ant-radio-disabled + span {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-radio-button-wrapper {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-radio-button-wrapper:hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-radio-button-wrapper-checked:not(.ant-radio-button-wrapper-disabled) {
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-radio-button-wrapper-disabled {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }

    /* ── Alert ────────────────────────────────────────────── */
    .ant-alert {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-alert-error {
        background-color: ${(props) => props.theme.colors.bgSurfaceError} !important;
        border-color: ${(props) => props.theme.colors.borderError} !important;
    }
    .ant-alert-warning {
        background-color: ${(props) => props.theme.colors.bgSurfaceWarning} !important;
        border-color: ${(props) => props.theme.colors.borderWarning} !important;
    }
    .ant-alert-info {
        background-color: ${(props) => props.theme.colors.bgSurfaceInfo} !important;
        border-color: ${(props) => props.theme.colors.borderInformation} !important;
    }
    .ant-alert-success {
        background-color: ${(props) => props.theme.colors.bgSurfaceSuccess} !important;
        border-color: ${(props) => props.theme.colors.borderSuccess} !important;
    }
    .ant-alert-message {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-alert-description {
        color: ${(props) => props.theme.colors.textSecondary} !important;
    }
    .ant-alert-error .ant-alert-icon {
        color: ${(props) => props.theme.colors.iconError};
    }
    .ant-alert-warning .ant-alert-icon {
        color: ${(props) => props.theme.colors.iconWarning};
    }
    .ant-alert-info .ant-alert-icon {
        color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-alert-success .ant-alert-icon {
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-alert-close-icon {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-alert-close-icon:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }

    /* ── Drawer ───────────────────────────────────────────── */
    .ant-drawer-mask {
        background-color: ${(props) => props.theme.colors.overlayHeavy};
    }
    .ant-drawer-content {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-drawer-header {
        background-color: ${(props) => props.theme.colors.bg};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-drawer-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-drawer-close {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-drawer-close:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-drawer-footer {
        border-top-color: ${(props) => props.theme.colors.border};
    }
    .ant-drawer-content-wrapper {
        box-shadow: ${(props) => props.theme.colors.shadowLg};
    }

    /* ── Buttons ──────────────────────────────────────────── */
    /* antd 4 renders default buttons as a bare .ant-btn, so this selector ties with every
       styled(Button) in the app and wins on injection order. Only declare what antd
       hardcodes to a light value; leave box-shadow alone so a component-level
       "box-shadow: none" keeps working. antd's own resting shadow is
       effectively transparent and needs no dark-mode equivalent. */
    .ant-btn {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-btn-default {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-btn:hover,
    .ant-btn:focus,
    .ant-btn-default:hover,
    .ant-btn-default:focus,
    .ant-btn-dashed:hover,
    .ant-btn-dashed:focus {
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-btn:active,
    .ant-btn-default:active,
    .ant-btn-dashed:active {
        border-color: ${(props) => props.theme.colors.borderActive};
        color: ${(props) => props.theme.colors.textActive};
    }
    .ant-btn-primary {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }
    .ant-btn-primary:hover,
    .ant-btn-primary:focus {
        background-color: ${(props) => props.theme.colors.buttonSurfaceBrandHover};
        border-color: ${(props) => props.theme.colors.borderHover};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }
    .ant-btn-primary:active {
        background-color: ${(props) => props.theme.colors.buttonFillFocus};
        border-color: ${(props) => props.theme.colors.borderActive};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }
    .ant-btn-dangerous,
    .ant-btn-dangerous.ant-btn-link,
    .ant-btn-dangerous.ant-btn-text {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-btn-primary.ant-btn-dangerous {
        background-color: ${(props) => props.theme.colors.iconError};
        border-color: ${(props) => props.theme.colors.borderError};
        color: ${(props) => props.theme.colors.textOnFillError};
    }
    .ant-btn-dangerous:hover,
    .ant-btn-dangerous:focus,
    .ant-btn-primary.ant-btn-dangerous:hover,
    .ant-btn-primary.ant-btn-dangerous:focus {
        background-color: ${(props) => props.theme.colors.bgSurfaceErrorHover};
        border-color: ${(props) => props.theme.colors.borderError};
        color: ${(props) => props.theme.colors.textOnFillError};
    }
    .ant-btn[disabled],
    .ant-btn[disabled]:hover,
    .ant-btn[disabled]:focus,
    .ant-btn[disabled]:active {
        background-color: ${(props) => props.theme.colors.bgSurfaceDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }
    /* Link and text buttons are chromeless in antd, but they also carry .ant-btn.
       The base rule above declares a fill and border at the same specificity as antd's
       own transparent declarations, so source order would otherwise repaint them as
       default buttons. Hover/focus/active are listed explicitly because .ant-btn:hover
       outranks a plain-class reset. */
    .ant-btn-link,
    .ant-btn-link:hover,
    .ant-btn-link:focus,
    .ant-btn-link:active,
    .ant-btn-text,
    .ant-btn-text:hover,
    .ant-btn-text:focus,
    .ant-btn-text:active {
        border-color: transparent;
    }
    .ant-btn-link,
    .ant-btn-link:hover,
    .ant-btn-link:focus,
    .ant-btn-link:active,
    .ant-btn-text {
        background-color: transparent;
    }
    .ant-btn-text {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-btn-text:hover,
    .ant-btn-text:focus {
        background-color: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-btn-text:active {
        background-color: ${(props) => props.theme.colors.bgActive};
        color: ${(props) => props.theme.colors.textActive};
    }
    .ant-btn-link {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
    .ant-btn-link:hover,
    .ant-btn-link:focus {
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-btn-link:active {
        color: ${(props) => props.theme.colors.textActive};
    }
    .ant-btn-background-ghost {
        background-color: transparent;
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-btn-group .ant-btn {
        border-color: ${(props) => props.theme.colors.border};
    }

    /* ── Pagination ───────────────────────────────────────── */
    .ant-pagination {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    .ant-pagination-item {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-pagination-item a {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-pagination-item-active {
        background-color: ${(props) => props.theme.colors.bgSelectedSubtle};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-pagination-item:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-pagination-prev .ant-pagination-item-link,
    .ant-pagination-next .ant-pagination-item-link {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-pagination-disabled .ant-pagination-item-link,
    .ant-pagination-disabled:hover .ant-pagination-item-link {
        background-color: ${(props) => props.theme.colors.bgSurfaceDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-pagination-item-link-icon,
    .ant-pagination-item-active a,
    .ant-pagination-item-active:hover a {
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-pagination-options-quick-jumper input {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-pagination-simple .ant-pagination-simple-pager input {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Breadcrumb ───────────────────────────────────────── */
    .ant-breadcrumb {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    .ant-breadcrumb a {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-breadcrumb-separator {
        color: ${(props) => props.theme.colors.textTertiary};
    }

    /* ── Badge ────────────────────────────────────────────── */
    .ant-badge-count {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        color: ${(props) => props.theme.colors.textOnFillBrand};
        box-shadow: 0 0 0 1px ${(props) => props.theme.colors.bg};
    }
    .ant-badge,
    .ant-badge-count a {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-badge-dot {
        background-color: ${(props) => props.theme.colors.iconError};
        box-shadow: 0 0 0 1px ${(props) => props.theme.colors.bg};
    }
    .ant-badge-status-text {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-badge-status-default {
        background-color: ${(props) => props.theme.colors.icon};
    }
    .ant-badge-status-processing {
        background-color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-badge-status-processing::after {
        border-color: ${(props) => props.theme.colors.borderInformation};
    }
    .ant-badge-status-success {
        background-color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-badge-status-error {
        background-color: ${(props) => props.theme.colors.iconError};
    }
    .ant-badge-status-warning {
        background-color: ${(props) => props.theme.colors.iconWarning};
    }

    /* ── Collapse / Accordion ─────────────────────────────── */
    .ant-collapse {
        background-color: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-collapse-ghost {
        background-color: transparent;
    }
    .ant-collapse > .ant-collapse-item {
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-collapse > .ant-collapse-item > .ant-collapse-header {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-collapse-content {
        background-color: ${(props) => props.theme.colors.bg};
        border-top-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-collapse-ghost > .ant-collapse-item > .ant-collapse-content {
        background-color: transparent;
    }
    .ant-collapse > .ant-collapse-item-disabled > .ant-collapse-header {
        color: ${(props) => props.theme.colors.textDisabled};
    }

    /* ── Steps ────────────────────────────────────────────── */
    .ant-steps-item-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-steps {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-steps-item-subtitle {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-steps-item-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-steps-item-icon {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-steps-item-icon > .ant-steps-icon,
    .ant-steps-item-wait .ant-steps-item-icon > .ant-steps-icon {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-steps-item-process .ant-steps-item-icon,
    .ant-steps-item-finish .ant-steps-item-icon {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-steps-item-process .ant-steps-item-icon > .ant-steps-icon,
    .ant-steps-item-finish .ant-steps-item-icon > .ant-steps-icon {
        color: ${(props) => props.theme.colors.iconOnFillBrand};
    }
    .ant-steps-item-error .ant-steps-item-icon {
        background-color: ${(props) => props.theme.colors.bgSurfaceError};
        border-color: ${(props) => props.theme.colors.borderError};
    }
    .ant-steps-item-error .ant-steps-item-icon > .ant-steps-icon,
    .ant-steps-item-error .ant-steps-item-title {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-steps-item-tail::after {
        background-color: ${(props) => props.theme.colors.border};
    }
    .ant-steps-item-finish > .ant-steps-item-container > .ant-steps-item-tail::after {
        background-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-steps-item-wait .ant-steps-item-title,
    .ant-steps-item-wait .ant-steps-item-description {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-steps-item-title::after {
        background-color: ${(props) => props.theme.colors.border};
    }
    .ant-steps-navigation .ant-steps-item::before {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
    }

    /* ── Empty ────────────────────────────────────────────── */
    .ant-empty-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-empty-normal,
    .ant-empty-small {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-empty-img-default-ellipse,
    .ant-empty-img-simple-ellipse {
        fill: ${(props) => props.theme.colors.bgSurfaceDisabled};
    }
    .ant-empty-img-default-path-1 {
        fill: ${(props) => props.theme.colors.iconDisabled};
    }
    .ant-empty-img-default-path-2 {
        fill: ${(props) => props.theme.colors.bgSkeleton};
    }
    .ant-empty-img-default-path-3,
    .ant-empty-img-default-path-4,
    .ant-empty-img-default-path-5,
    .ant-empty-img-simple-path {
        fill: ${(props) => props.theme.colors.bgSurface};
    }
    .ant-empty-img-default-g {
        fill: ${(props) => props.theme.colors.bg};
    }
    .ant-empty-img-simple-g {
        stroke: ${(props) => props.theme.colors.border};
    }

    /* ── Skeleton ─────────────────────────────────────────── */
    .ant-skeleton-content .ant-skeleton-title,
    .ant-skeleton-content .ant-skeleton-paragraph > li {
        background: ${(props) => props.theme.colors.bgSkeleton};
    }
    .ant-skeleton-active .ant-skeleton-content .ant-skeleton-title,
    .ant-skeleton-active .ant-skeleton-content .ant-skeleton-paragraph > li {
        background: linear-gradient(
            90deg,
            ${(props) => props.theme.colors.bgSkeleton} 25%,
            ${(props) => props.theme.colors.bgSkeletonShimmer} 37%,
            ${(props) => props.theme.colors.bgSkeleton} 63%
        );
        background-size: 400% 100%;
    }
    .ant-skeleton-avatar,
    .ant-skeleton-button,
    .ant-skeleton-input,
    .ant-skeleton-image {
        background-color: ${(props) => props.theme.colors.bgSkeleton};
    }
    .ant-skeleton-image-svg,
    .ant-skeleton-element .ant-skeleton-image-path {
        color: ${(props) => props.theme.colors.iconDisabled};
        fill: ${(props) => props.theme.colors.iconDisabled};
    }
    .ant-skeleton-active .ant-skeleton-avatar::after,
    .ant-skeleton-active .ant-skeleton-button::after,
    .ant-skeleton-active .ant-skeleton-input::after,
    .ant-skeleton-active .ant-skeleton-image::after {
        background: linear-gradient(
            90deg,
            ${(props) => props.theme.colors.bgSkeleton} 25%,
            ${(props) => props.theme.colors.bgSkeletonShimmer} 37%,
            ${(props) => props.theme.colors.bgSkeleton} 63%
        );
    }

    /* ── Notification ─────────────────────────────────────── */
    .ant-notification-notice {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowLg};
    }
    .ant-notification {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-notification-notice-message {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-notification-notice-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-notification-notice-close {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-notification-notice-close:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-notification-notice-icon-success {
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-notification-notice-icon-info {
        color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-notification-notice-icon-warning {
        color: ${(props) => props.theme.colors.iconWarning};
    }
    .ant-notification-notice-icon-error {
        color: ${(props) => props.theme.colors.iconError};
    }

    /* ── Message ──────────────────────────────────────────── */
    .ant-message-notice-content {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    body .ant-message-notice-content .ant-message-success {
        background-color: ${(props) => props.theme.colors.bgSurfaceSuccess};
        border-left-color: ${(props) => props.theme.colors.borderSuccess};
        color: ${(props) => props.theme.colors.textOnSurfaceSuccess};
    }
    body .ant-message-notice-content .ant-message-error {
        background-color: ${(props) => props.theme.colors.bgSurfaceError};
        border-left-color: ${(props) => props.theme.colors.borderError};
        color: ${(props) => props.theme.colors.textOnSurfaceError};
    }
    body .ant-message-notice-content .ant-message-warning {
        background-color: ${(props) => props.theme.colors.bgSurfaceWarning};
        border-left-color: ${(props) => props.theme.colors.borderWarning};
        color: ${(props) => props.theme.colors.textOnSurfaceWarning};
    }
    body .ant-message-notice-content .ant-message-info {
        background-color: ${(props) => props.theme.colors.bgSurfaceInfo};
        border-left-color: ${(props) => props.theme.colors.borderInformation};
        color: ${(props) => props.theme.colors.textOnSurfaceInformation};
    }
    body .ant-message-notice-content .ant-message-success .anticon {
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    body .ant-message-notice-content .ant-message-error .anticon {
        color: ${(props) => props.theme.colors.iconError};
    }
    body .ant-message-notice-content .ant-message-warning .anticon {
        color: ${(props) => props.theme.colors.iconWarning};
    }
    body .ant-message-notice-content .ant-message-info .anticon {
        color: ${(props) => props.theme.colors.iconInformation};
    }

    /* ── Spin ─────────────────────────────────────────────── */
    .ant-spin-text {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-spin {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-spin-dot-item {
        background-color: ${(props) => props.theme.colors.iconBrand};
    }
    .ant-spin-container::after {
        background-color: ${(props) => props.theme.colors.overlayLight};
    }

    /* ── Switch ───────────────────────────────────────────── */
    .ant-switch {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    .ant-switch.ant-switch-checked {
        background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
    }
    .ant-switch-handle::before {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
    .ant-switch-disabled {
        background-color: ${(props) => props.theme.colors.bgSurfaceDisabled};
    }
    .ant-switch-inner {
        color: ${(props) => props.theme.colors.textOnFillDefault};
    }
    .ant-switch:focus {
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }
    .ant-switch.ant-switch-checked:focus {
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
    .ant-switch-loading-icon {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-switch-checked .ant-switch-loading-icon {
        color: ${(props) => props.theme.colors.iconBrand};
    }

    /* ── Tooltip ──────────────────────────────────────────── */
    .ant-tooltip-inner {
        background-color: ${(props) => props.theme.colors.bgSurfaceDarker};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-tooltip-arrow-content {
        background-color: ${(props) => props.theme.colors.bgSurfaceDarker};
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }

    /* ── Segmented ────────────────────────────────────────── */
    .ant-segmented {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-segmented-item-selected {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Date / Time Picker ───────────────────────────────── */
    .ant-picker,
    .ant-picker-input > input {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-picker:hover,
    .ant-picker-focused,
    .ant-picker-input > input:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-picker-focused,
    .ant-picker-input > input:focus {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
    .ant-picker-status-error.ant-picker,
    .ant-picker-status-error.ant-picker:not([disabled]):hover {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderError};
    }
    .ant-picker-status-error.ant-picker-focused,
    .ant-picker-status-error.ant-picker:focus {
        border-color: ${(props) => props.theme.colors.borderError};
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }
    .ant-picker-status-error.ant-picker .ant-picker-active-bar {
        background-color: ${(props) => props.theme.colors.borderError};
    }
    .ant-picker-status-warning.ant-picker,
    .ant-picker-status-warning.ant-picker:not([disabled]):hover {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderWarning};
    }
    .ant-picker-status-warning.ant-picker-focused,
    .ant-picker-status-warning.ant-picker:focus {
        border-color: ${(props) => props.theme.colors.borderWarning};
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }
    .ant-picker-status-warning.ant-picker .ant-picker-active-bar {
        background-color: ${(props) => props.theme.colors.borderWarning};
    }
    .ant-picker.ant-picker-disabled,
    .ant-picker-input > input[disabled] {
        background-color: ${(props) => props.theme.colors.bgInputDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-picker-input > input::placeholder,
    .ant-picker-input-placeholder > input {
        color: ${(props) => props.theme.colors.textPlaceholder};
    }
    .ant-picker-suffix,
    .ant-picker-separator,
    .ant-picker-clear,
    .ant-picker.ant-picker-disabled .ant-picker-suffix {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-picker-clear {
        background-color: ${(props) => props.theme.colors.bgInput};
    }
    .ant-picker-clear:hover,
    .ant-picker-focused .ant-picker-separator {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-picker-range .ant-picker-active-bar {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
    }
    .ant-picker-dropdown,
    .ant-picker-header,
    .ant-picker-content th,
    .ant-picker-time-panel-cell-inner {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-picker-panel-container,
    .ant-picker-panel {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-picker-panel-container {
        box-shadow: ${(props) => props.theme.colors.shadowLg};
    }
    .ant-picker-range-arrow {
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
    .ant-picker-range-arrow::before {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-picker-header,
    .ant-picker-panel .ant-picker-footer,
    .ant-picker-footer-extra:not(:last-child),
    .ant-picker-datetime-panel .ant-picker-time-panel,
    .ant-picker-time-panel-column:not(:first-child) {
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-picker-header button {
        color: ${(props) => props.theme.colors.icon};
    }
    .ant-picker-header > button:hover,
    .ant-picker-header-view button:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
    .ant-picker-cell {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-picker-cell-in-view {
        color: ${(props) => props.theme.colors.text};
    }
    body
        .ant-picker-cell:hover:not(.ant-picker-cell-selected):not(.ant-picker-cell-range-start):not(
            .ant-picker-cell-range-end
        ):not(.ant-picker-cell-range-hover-start):not(.ant-picker-cell-range-hover-end)
        .ant-picker-cell-inner,
    body .ant-picker-week-panel-row:hover td,
    body .ant-picker-time-panel-cell-inner:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-picker-cell-in-view.ant-picker-cell-today .ant-picker-cell-inner::before {
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    .ant-picker-cell-in-view.ant-picker-cell-in-range::before,
    .ant-picker-cell-in-view.ant-picker-cell-range-start:not(.ant-picker-cell-range-start-single)::before,
    .ant-picker-cell-in-view.ant-picker-cell-range-end:not(.ant-picker-cell-range-end-single)::before,
    .ant-picker-time-panel-column-active,
    .ant-picker-time-panel-cell-selected .ant-picker-time-panel-cell-inner {
        background-color: ${(props) => props.theme.colors.bgSelectedSubtle};
    }
    .ant-picker-cell-in-view.ant-picker-cell-selected .ant-picker-cell-inner,
    .ant-picker-cell-in-view.ant-picker-cell-range-start .ant-picker-cell-inner,
    .ant-picker-cell-in-view.ant-picker-cell-range-end .ant-picker-cell-inner,
    .ant-picker-week-panel-row-selected td,
    .ant-picker-week-panel-row-selected:hover td {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        color: ${(props) => props.theme.colors.textOnFillBrand};
    }
    .ant-picker-cell-range-hover::after,
    .ant-picker-cell-range-hover-start::after,
    .ant-picker-cell-range-hover-end::after {
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
    body .ant-picker-cell-in-view.ant-picker-cell-in-range.ant-picker-cell-range-hover::before,
    body .ant-picker-cell-in-view.ant-picker-cell-range-start.ant-picker-cell-range-hover::before,
    body .ant-picker-cell-in-view.ant-picker-cell-range-end.ant-picker-cell-range-hover::before,
    body
        .ant-picker-date-panel
        .ant-picker-cell-in-view.ant-picker-cell-in-range.ant-picker-cell-range-hover-start
        .ant-picker-cell-inner::after,
    body
        .ant-picker-date-panel
        .ant-picker-cell-in-view.ant-picker-cell-in-range.ant-picker-cell-range-hover-end
        .ant-picker-cell-inner::after {
        background-color: ${(props) => props.theme.colors.bgSelected};
    }
    .ant-picker-cell-disabled,
    .ant-picker-time-panel-cell-disabled .ant-picker-time-panel-cell-inner,
    .ant-picker-today-btn.ant-picker-today-btn-disabled {
        color: ${(props) => props.theme.colors.textDisabled};
    }
    .ant-picker-cell-disabled::before {
        background-color: ${(props) => props.theme.colors.bgDisabled};
    }
    .ant-picker-cell-disabled.ant-picker-cell-today .ant-picker-cell-inner::before {
        border-color: ${(props) => props.theme.colors.borderDisabled};
    }
    .ant-picker-today-btn {
        color: ${(props) => props.theme.colors.textBrand};
    }
    .ant-picker-today-btn:hover {
        color: ${(props) => props.theme.colors.textHover};
    }
    .ant-picker-today-btn:active {
        color: ${(props) => props.theme.colors.textActive};
    }
    .ant-picker-ranges .ant-picker-preset > .ant-tag-blue {
        background-color: ${(props) => props.theme.colors.tagsTrueBlueBg};
        border-color: ${(props) => props.theme.colors.tagsTrueBlueBorder};
        color: ${(props) => props.theme.colors.tagsTrueBlueText};
    }

    /* ── Result ───────────────────────────────────────────── */
    .ant-result-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-result-subtitle {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-result-content {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    .ant-result-success .ant-result-icon > .anticon {
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-result-error .ant-result-icon > .anticon {
        color: ${(props) => props.theme.colors.iconError};
    }
    .ant-result-info .ant-result-icon > .anticon {
        color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-result-warning .ant-result-icon > .anticon {
        color: ${(props) => props.theme.colors.iconWarning};
    }

    /* ── Timeline ─────────────────────────────────────────── */
    .ant-timeline {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-timeline-item-tail {
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-timeline-item-head {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-timeline-item-head-blue {
        border-color: ${(props) => props.theme.colors.borderInformation};
        color: ${(props) => props.theme.colors.iconInformation};
    }
    .ant-timeline-item-head-red {
        border-color: ${(props) => props.theme.colors.borderError};
        color: ${(props) => props.theme.colors.iconError};
    }
    .ant-timeline-item-head-green {
        border-color: ${(props) => props.theme.colors.borderSuccess};
        color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-timeline-item-head-gray {
        border-color: ${(props) => props.theme.colors.borderDisabled};
        color: ${(props) => props.theme.colors.iconDisabled};
    }

    /* ── Progress ─────────────────────────────────────────── */
    .ant-progress,
    .ant-progress-text,
    .ant-progress-circle .ant-progress-text {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-progress-steps-item,
    .ant-progress-inner {
        background-color: ${(props) => props.theme.colors.bgSurfaceDisabled};
    }
    .ant-progress-circle-trail {
        stroke: ${(props) => props.theme.colors.bgSurfaceDisabled};
    }
    .ant-progress-steps-item-active,
    .ant-progress-success-bg,
    .ant-progress-bg {
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
    }
    .ant-progress-inner:not(.ant-progress-circle-gradient) .ant-progress-circle-path {
        stroke: ${(props) => props.theme.colors.buttonFillBrand};
    }
    .ant-progress-status-active .ant-progress-bg::before {
        background-color: ${(props) => props.theme.colors.overlayOnBrand};
    }
    .ant-progress-status-exception .ant-progress-bg {
        background-color: ${(props) => props.theme.colors.iconError};
    }
    .ant-progress-status-exception .ant-progress-text {
        color: ${(props) => props.theme.colors.textError};
    }
    .ant-progress-status-exception .ant-progress-inner:not(.ant-progress-circle-gradient) .ant-progress-circle-path {
        stroke: ${(props) => props.theme.colors.iconError};
    }
    .ant-progress-status-success .ant-progress-bg {
        background-color: ${(props) => props.theme.colors.iconSuccess};
    }
    .ant-progress-status-success .ant-progress-text {
        color: ${(props) => props.theme.colors.textSuccess};
    }
    .ant-progress-status-success .ant-progress-inner:not(.ant-progress-circle-gradient) .ant-progress-circle-path {
        stroke: ${(props) => props.theme.colors.iconSuccess};
    }

    /* ── Carousel ─────────────────────────────────────────── */
    .ant-carousel {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-carousel .slick-dots li button,
    .ant-carousel .slick-dots li.slick-active button {
        background-color: ${(props) => props.theme.colors.textOnFillDefault};
    }

    /* ── Image preview ────────────────────────────────────── */
    .ant-image-img-placeholder {
        background-color: ${(props) => props.theme.colors.bgSkeleton};
        background-image: none;
    }
    .ant-image-mask {
        background-color: ${(props) => props.theme.colors.overlayMedium};
        color: ${(props) => props.theme.colors.textOnFillDefault};
    }
    .ant-image-preview-mask {
        background-color: ${(props) => props.theme.colors.overlayHeavy};
    }
    .ant-image-preview-operations,
    .ant-image-preview-switch-left,
    .ant-image-preview-switch-right {
        background-color: ${(props) => props.theme.colors.overlayLight};
        color: ${(props) => props.theme.colors.textOnFillDefault};
    }
    .ant-image-preview-operations-operation:hover,
    .ant-image-preview-switch-left:hover,
    .ant-image-preview-switch-right:hover {
        background-color: ${(props) => props.theme.colors.overlayMedium};
    }
    .ant-image-preview-operations-operation-disabled,
    .ant-image-preview-switch-left-disabled,
    .ant-image-preview-switch-right-disabled,
    .ant-image-preview-switch-left-disabled:hover,
    .ant-image-preview-switch-right-disabled:hover {
        background-color: ${(props) => props.theme.colors.overlayLight};
        color: ${(props) => props.theme.colors.textDisabled};
    }

    /* ── Scrollbar ────────────────────────────────────────── */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }
    ::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }

    /* ── Links ────────────────────────────────────────────── */
    a {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

export default GlobalThemeStyles;
