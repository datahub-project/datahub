import { css } from 'styled-components';

/**
 * Repaints the backgrounds that `react-js-cron`'s shipped `dist/styles.css`
 * hardcodes as light-mode colors.
 *
 * The library paints the wrapper around its hour/minute selects `white`, and in
 * the very next rule forces the antd `.ant-select-selector` inside them to
 * `transparent`. Our global antd theming only reaches that selector, so it has
 * no way to override the white — the wrapper itself has to be repainted. The
 * disabled (`#f5f5f5`) and error (`#fff6f6`) fills have the same problem.
 *
 * Compose this into the styled container that already wraps a `<Cron />`.
 */
export const cronBuilderStyles = css`
    .react-js-cron-custom-select {
        background: ${(props) => props.theme.colors.bgInput};
    }

    .react-js-cron-disabled .react-js-cron-select.ant-select-disabled,
    .react-js-cron-disabled .react-js-cron-custom-select {
        background: ${(props) => props.theme.colors.bgInputDisabled};
    }

    .react-js-cron-error .react-js-cron-custom-select,
    .react-js-cron-error .react-js-cron-select .ant-select-selector {
        background: ${(props) => props.theme.colors.bgSurfaceError};
        border-color: ${(props) => props.theme.colors.borderError};
    }
`;
