

# Multi-Language Support

> **Availability:** Self-Hosted DataHub & DataHub Cloud

DataHub's UI can be displayed in multiple languages. Each user chooses their preferred language from
their personal settings, so people on the same instance can use DataHub in whichever language they're
most comfortable with.

## Available Languages

- English (default)
- German (Deutsch)
- Spanish (Español) — Beta
- Portuguese, Brazil (Português) — Beta
- French (Français) — Beta
- Italian (Italiano) — Beta
- Norwegian (Norsk bokmål) — Beta
- Swedish (Svenska) — Beta
- Hungarian (Magyar) — Beta

Languages marked _Beta_ are still being refined and may have untranslated strings.

## Enabling Multi-Language Support

Multi-language support is on by default. On first visit, DataHub picks each user's language
from their browser locale, falling back to English when no matching translation is available.
Each user can override this under **Settings → Preferences**. To turn multi-language support
off entirely, set the `I18N_ENABLED` environment variable to `false` on GMS and restart.

## Contributing a New Language

DataHub is open source, and we welcome community contributions for new languages as well as
improvements to existing translations. If a language you need isn't listed above — or you spot a
translation that could be better — you can add or update it and open a pull request.

Translation files live under `datahub-web-react/src/i18n/locales/<language>/` in the
[DataHub repository](https://github.com/datahub-project/datahub). See the
[Contributing Guide](../../CONTRIBUTING.md) to get started, and reach out on
[Slack](https://datahubspace.slack.com) if you'd like to coordinate on a new language.
