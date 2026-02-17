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
    body {
        background-color: ${(props) => props.theme.colors.bgSurfaceNewNav};
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Ant Layout ───────────────────────────────────────── */
    .ant-layout {
        background-color: transparent;
    }

    /* ── Modals ───────────────────────────────────────────── */
    .ant-modal-content {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-modal-header {
        background-color: ${(props) => props.theme.colors.bg};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-modal-header .ant-modal-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-modal-footer {
        border-top-color: ${(props) => props.theme.colors.border};
    }
    .ant-modal-body {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-modal-close-x {
        color: ${(props) => props.theme.colors.icon};
    }

    /* ── Dropdowns ────────────────────────────────────────── */
    .ant-dropdown-menu {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-dropdown-menu-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-dropdown-menu-item:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-select-dropdown {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-select-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-select-item-option-active:not(.ant-select-item-option-disabled) {
        background-color: ${(props) => props.theme.colors.bgHover};
    }

    /* ── Inputs ───────────────────────────────────────────── */
    .ant-input {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-input::placeholder {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    .ant-select:not(.ant-select-customize-input) .ant-select-selector {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-input-affix-wrapper {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
    }

    /* ── Tables ───────────────────────────────────────────── */
    .ant-table {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-table-thead > tr > th {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.text};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-table-tbody > tr > td {
        border-bottom-color: ${(props) => props.theme.colors.border};
    }
    .ant-table-tbody > tr:hover > td {
        background-color: ${(props) => props.theme.colors.bgHover};
    }

    /* ── Cards ────────────────────────────────────────────── */
    .ant-card {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-card-head {
        color: ${(props) => props.theme.colors.text};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }

    /* ── Popover / Tooltip ────────────────────────────────── */
    .ant-popover-inner {
        background-color: ${(props) => props.theme.colors.bg};
    }
    .ant-popover-inner-content {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-popover-title {
        color: ${(props) => props.theme.colors.text};
        border-bottom-color: ${(props) => props.theme.colors.border};
    }

    /* ── Tabs ─────────────────────────────────────────────── */
    .ant-tabs {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-tabs-tab {
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-tabs-tab:hover {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${(props) => props.theme.styles['primary-color']};
    }

    /* ── List ─────────────────────────────────────────────── */
    .ant-list {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-list-item {
        border-bottom-color: ${(props) => props.theme.colors.border};
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

    /* ── Divider ──────────────────────────────────────────── */
    .ant-divider {
        border-top-color: ${(props) => props.theme.colors.border};
    }

    /* ── Typography ───────────────────────────────────────── */
    h1, h2, h3, h4, h5, h6,
    .ant-typography {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Tag ──────────────────────────────────────────────── */
    .ant-tag {
        border-color: ${(props) => props.theme.colors.border};
    }

    /* ── Form ─────────────────────────────────────────────── */
    .ant-form-item-label > label {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Checkbox ─────────────────────────────────────────── */
    .ant-checkbox-wrapper {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Radio ────────────────────────────────────────────── */
    .ant-radio-wrapper {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Alert ────────────────────────────────────────────── */
    .ant-alert {
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Drawer ───────────────────────────────────────────── */
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

    /* ── Buttons ──────────────────────────────────────────── */
    .ant-btn-default {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-btn-default:hover {
        border-color: ${(props) => props.theme.styles['primary-color']};
        color: ${(props) => props.theme.styles['primary-color']};
    }
    .ant-btn-text {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-btn-link {
        color: ${(props) => props.theme.styles['primary-color']};
    }

    /* ── Pagination ───────────────────────────────────────── */
    .ant-pagination-item {
        background-color: ${(props) => props.theme.colors.bg};
        border-color: ${(props) => props.theme.colors.border};
    }
    .ant-pagination-item a {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-pagination-item-active {
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
    .ant-pagination-prev .ant-pagination-item-link,
    .ant-pagination-next .ant-pagination-item-link {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.border};
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
        box-shadow: 0 0 0 1px ${(props) => props.theme.colors.bg};
    }

    /* ── Collapse / Accordion ─────────────────────────────── */
    .ant-collapse {
        background-color: ${(props) => props.theme.colors.bgSurface};
        border-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.text};
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

    /* ── Steps ────────────────────────────────────────────── */
    .ant-steps-item-title {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-steps-item-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }

    /* ── Empty ────────────────────────────────────────────── */
    .ant-empty-description {
        color: ${(props) => props.theme.colors.textSecondary};
    }

    /* ── Skeleton ─────────────────────────────────────────── */
    .ant-skeleton-content .ant-skeleton-title,
    .ant-skeleton-content .ant-skeleton-paragraph > li {
        background: ${(props) => props.theme.colors.bgSurface};
    }

    /* ── Notification ─────────────────────────────────────── */
    .ant-notification-notice {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
    }

    /* ── Message ──────────────────────────────────────────── */
    .ant-message-notice-content {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08);
    }

    /* ── Spin ─────────────────────────────────────────────── */
    .ant-spin-text {
        color: ${(props) => props.theme.colors.textSecondary};
    }

    /* ── Switch ───────────────────────────────────────────── */
    .ant-switch {
        background-color: ${(props) => props.theme.colors.textDisabled};
    }

    /* ── Tooltip ──────────────────────────────────────────── */
    .ant-tooltip-inner {
        background-color: ${(props) => props.theme.colors.bgSurfaceDarker};
        color: ${(props) => props.theme.colors.text};
    }
    .ant-tooltip-arrow-content {
        background-color: ${(props) => props.theme.colors.bgSurfaceDarker};
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

    /* ── Scrollbar (for dark mode) ────────────────────────── */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.textDisabled};
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.textTertiary};
    }

    /* ── Links ────────────────────────────────────────────── */
    a {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

export default GlobalThemeStyles;
