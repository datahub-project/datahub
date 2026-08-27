/**
 * ESLint rule: no-manual-pluralize-in-i18n
 *
 * Disallows the manual pluralization helpers from `@app/shared/textUtil`
 * (`pluralize`, `forcePluralize`, `pluralizeIfIrregular`) in files that have
 * already been migrated to i18n.
 *
 * These helpers hardcode English plural rules — a naive `+s` suffix plus a
 * small English-only irregular map (query→queries, match→matches, …). Using
 * them inside a translated file produces text that can never be localized
 * correctly: other locales have different plural categories (zero/one/few/
 * many/other) and entirely different word forms.
 *
 * The i18next-native approach is to pass `count` and let the locale files own
 * each plural form via suffixed keys:
 *
 *   // locale JSON
 *   "trialEndsIn_one":   "Your trial ends in {{count}} day",
 *   "trialEndsIn_other": "Your trial ends in {{count}} days"
 *
 *   // component
 *   <Trans i18nKey="trialEndsIn" count={days} />
 *   // or: t('trialEndsIn', { count: days })
 *
 * This rule is intentionally scoped (via .eslintrc.js) to the i18n allowlist so
 * it only fires on files that are expected to be fully translated.
 */

const TEXT_UTIL_SOURCE_REGEX = /(^|\/)shared\/textUtil$/;
const RESTRICTED_NAMES = new Set(['pluralize', 'forcePluralize', 'pluralizeIfIrregular']);

const MESSAGE =
    "Don't use `{{name}}` from textUtil in i18n'ed files — it hardcodes English plural rules. " +
    'Pass `count` to t()/<Trans> and define `_one`/`_other` (etc.) keys in the locale JSON so each language owns its plural forms.';

module.exports = {
    meta: {
        type: 'problem',
        docs: {
            description:
                'Disallow manual pluralization helpers (pluralize/forcePluralize/pluralizeIfIrregular) in i18n-translated files. Use i18next count-based plural keys instead.',
        },
        schema: [],
        messages: {
            noManualPluralize: MESSAGE,
        },
    },
    create(context) {
        return {
            ImportDeclaration(node) {
                if (typeof node.source.value !== 'string') return;
                if (!TEXT_UTIL_SOURCE_REGEX.test(node.source.value)) return;

                node.specifiers.forEach((specifier) => {
                    if (specifier.type !== 'ImportSpecifier') return;
                    const importedName = specifier.imported && specifier.imported.name;
                    if (!RESTRICTED_NAMES.has(importedName)) return;

                    context.report({
                        node: specifier,
                        messageId: 'noManualPluralize',
                        data: { name: importedName },
                    });
                });
            },
        };
    },
};
