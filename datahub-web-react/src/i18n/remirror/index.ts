import type { Messages } from '@remirror/i18n';

import de from '@src/i18n/remirror/de.json';
import ptBR from '@src/i18n/remirror/pt-BR.json';

// Supplementary locale bundles for Remirror's built-in labels (command/table/keyboard),
// keyed by language code and loaded directly into Remirror's own Lingui i18n (see
// `RemirrorLocaleProvider`). English ships with `@remirror/i18n`, so it is intentionally
// not duplicated here. Values may be plain strings or ICU strings (plural/select, e.g.
// `{count, plural, one {# Zeile} other {# Zeilen}}`) that Lingui compiles at render time.
//
// NOTE: This is a Lingui catalog for Remirror, NOT an i18next namespace — keep it out of
// `src/i18n/locales/`, which is reserved for i18next resources. Each bundle MUST cover
// every key Remirror defines for `en` (Lingui renders the raw message id for a missing
// key); this is enforced by `__tests__/remirror.test.ts`.
export const REMIRROR_LOCALE_MESSAGES: Record<string, Messages> = { de, 'pt-BR': ptBR };
