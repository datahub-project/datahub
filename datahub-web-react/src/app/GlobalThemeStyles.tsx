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
        --theme-shadowFocus: ${(props) => props.theme.colors.shadowFocus};
        --theme-shadowFocusBrand: ${(props) => props.theme.colors.shadowFocusBrand};
        --theme-overlayLight: ${(props) => props.theme.colors.overlayLight};
        --theme-overlayMedium: ${(props) => props.theme.colors.overlayMedium};
        --theme-overlayHeavy: ${(props) => props.theme.colors.overlayHeavy};
    }

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
        background-color: ${(props) => props.theme.colors.bg} !important;
        color: ${(props) => props.theme.colors.text} !important;
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
    .ant-modal-mask {
        background-color: ${(props) => props.theme.colors.overlayHeavy};
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
    .ant-dropdown-menu {
        background-color: ${(props) => props.theme.colors.bg};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }
    .ant-dropdown-menu-item {
        color: ${(props) => props.theme.colors.text};
    }
    .ant-dropdown-menu-item:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    .ant-select-dropdown {
        background-color: ${(props) => props.theme.colors.bg};
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

    /* ── Inputs ───────────────────────────────────────────── */
    .ant-input {
        background-color: ${(props) => props.theme.colors.bgInput};
        color: ${(props) => props.theme.colors.text};
        border-color: ${(props) => props.theme.colors.borderInput};
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
    }
    .ant-input-affix-wrapper {
        background-color: ${(props) => props.theme.colors.bgInput};
        border-color: ${(props) => props.theme.colors.borderInput};
    }
    .ant-input-affix-wrapper:hover {
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    .ant-input-affix-wrapper-focused {
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
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
        box-shadow: ${(props) => props.theme.colors.shadowMd};
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
    .ant-menu-item-selected {
        background-color: ${(props) => props.theme.colors.bgSelected};
        color: ${(props) => props.theme.colors.textSelected};
    }

    /* ── Divider ──────────────────────────────────────────── */
    .ant-divider {
        border-top-color: ${(props) => props.theme.colors.border};
    }

    /* ── Typography ───────────────────────────────────────── */
    h1, h2, h3, h4, h5, h6,
    .ant-typography {
        color: ${(props) => props.theme.colors.text} !important;
    }
    .ant-typography.ant-typography-secondary {
        color: ${(props) => props.theme.colors.textSecondary} !important;
    }
    .ant-typography h5,
    h5.ant-typography {
        color: ${(props) => props.theme.colors.text} !important;
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

    /* ── Notification ─────────────────────────────────────── */
    .ant-notification-notice {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowLg};
    }

    /* ── Message ──────────────────────────────────────────── */
    .ant-message-notice-content {
        background-color: ${(props) => props.theme.colors.bg};
        color: ${(props) => props.theme.colors.text};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
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
