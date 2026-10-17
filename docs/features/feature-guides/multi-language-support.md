

# Multi-Language Support

> **Availability:** DataHub Core (OSS) & DataHub Cloud

DataHub's UI can be displayed in multiple languages. When multi-language support is enabled, each
user gets the language matching their browser automatically, and can switch to another one in their
personal settings. People on the same instance can use DataHub in whichever language they're most
comfortable with.

## Available Languages

- English (default)
- German (Deutsch)
- Spanish (Español) — Beta
- Portuguese, Brazil (Português) — Beta
- French (Français) — Beta
- Italian (Italiano) — Beta
- Norwegian (Norsk bokmål) — Beta
- Swedish (Svenska) — Beta
- Finnish (Suomi) — Beta
- Hungarian (Magyar) — Beta
- Japanese (日本語) — Beta
- Russian (Русский) — Beta
- Simplified Chinese (简体中文)
- Traditional Chinese (繁體中文) — Beta

Languages marked _Beta_ are still being refined and may have untranslated strings.

## What Is Translated

Translation covers DataHub's own interface: navigation, buttons, labels, dialogs, empty states,
and settings pages. Dates and times follow the selected language, and ingestion schedules are
shown as human-readable descriptions in that language (for example, "Every day at 9:00 AM"
becomes its equivalent in the chosen language). Menus in the documentation editor are translated
as well.

Some things stay in their original language:

- **Your metadata.** Asset names, descriptions, tags, glossary terms, domains, column names, and
  platform names are stored exactly as they were entered or ingested, so they appear unchanged.
- **Server messages.** Errors and other messages returned by the DataHub server are in English.
- **This documentation site.** docs.datahub.com is English only.

Any string that has no translation yet falls back to English instead of showing a placeholder,
which is why Beta languages often show a mix of both languages. The browser locale is matched to
the closest supported language, so `de-AT` uses German, `pt-PT` uses Portuguese (Brazil), and
`zh-HK` uses Traditional Chinese.

When multi-language support is turned off, the language selector is hidden and everyone sees
English, regardless of their browser locale or a previously saved preference.

## How Languages Are Chosen

Nothing to turn on. Multi-language support is enabled by default, and users never have to
activate it: on the first visit, DataHub picks each person's language from their browser locale,
falling back to English when no matching translation is available. Anyone who wants a different
language can change it under **Settings → Preferences**; that choice is saved to their profile and
overrides browser detection on every later visit.

To turn multi-language support off for the whole instance, set the `I18N_ENABLED` environment
variable to `false` on GMS and restart.

## Contributing a New Language

DataHub is open source, and we welcome community contributions for new languages as well as
improvements to existing translations. If a language you need isn't listed above — or you spot a
translation that could be better — you can add or update it and open a pull request.

Translation files live under `datahub-web-react/src/i18n/locales/<language>/` in the
[DataHub repository](https://github.com/datahub-project/datahub). See the
[Contributing Guide](../../CONTRIBUTING.md) to get started, and reach out on
[Slack](https://datahubspace.slack.com) if you'd like to coordinate on a new language.
