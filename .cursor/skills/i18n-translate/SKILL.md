---
name: i18n-translate
description: Use when generating translations for a new locale from existing EN namespace JSON files, doing a quality pass across all translations for a locale, or adding a new language to DataHub.
---

# i18n Translation Generation

Generate translations for a target locale from EN source JSON files, or audit existing translations for consistency.

## Generate translations

### Input

- Source locale files: `en/<namespace>.json`
- Target locale code: `de`, `es`, `fr`, `ja`, etc.
- Translation context: `src/i18n/context/datahub-translation-context.md`

### Rules (all locales)

1. **Preserve all `{{varName}}` placeholders exactly** — same name, same position, same count
2. **Do-not-translate glossary:** DataHub, lineage, ingestion, pipeline, schema, assertion, incident, Dataset, DataFlow, DataJob, Dashboard, Chart, MLModel, GlossaryTerm, Domain, Tag, Owner, DataProduct
3. **Preserve HTML tags** — do not translate attribute values
4. **Match key structure exactly** — same keys, same order, no additions or removals
5. **Pluralization** — include all CLDR plural categories required by target locale

### Locale-specific tone

<!-- markdownlint-disable MD060 -->

| Locale  | Register           | Notes                                      |
| ------- | ------------------ | ------------------------------------------ |
| `de`    | Formal "Sie"       | Never "du". Nouns capitalized per grammar. |
| `fr`    | Formal "vous"      | Never "tu".                                |
| `es`    | Formal register    | Where appropriate.                         |
| `pt-br` | Informal "voce"    | Acceptable.                                |
| `ja`    | Polite (desu/masu) | Standard software UI register.             |

<!-- markdownlint-enable MD060 -->

### Character budgets

<!-- markdownlint-disable MD060 -->

| UI element                     | Max chars | If over budget                           |
| ------------------------------ | --------- | ---------------------------------------- |
| Button label                   | 20        | Shorter synonym or standard abbreviation |
| Tab label                      | 25        | Single noun or abbreviation              |
| Table header                   | 30        | Abbreviate, rely on tooltip              |
| Menu item                      | 35        | Shorten                                  |
| Toast / error / tooltip / body | unlimited | No constraint                            |

<!-- markdownlint-enable MD060 -->

To determine element type: check the component source where the key is used.

### Output

- `<locale>/<namespace>.json` — keys in same order as EN, values translated

## Consistency audit

When auditing all translation files for a locale, check:

1. **Terminology consistency** — same English concept uses same translation everywhere. Build a term map (EN term -> target term), flag any term with 2+ different translations.
2. **Tone consistency** — formal register throughout, no informal slipping in
3. **Glossary compliance** — all do-not-translate terms preserved in English
4. **No contamination** — no mixed-language sentences ("Are you sure you want to Loschen?")
5. **Placeholder integrity** — all `{{varName}}` preserved exactly
6. **Character budget compliance** — spot-check buttons, tabs, table headers
7. **Plural form completeness** — all required CLDR categories present for the locale

Output a markdown report grouped by issue type, with file path and key for each finding.

## Adding a new locale

1. Create `src/i18n/locales/<locale>/`
2. Generate target JSON for every `en/*.json` file (including `saas.*` namespaces for SaaS builds)
3. Add locale to `supportedLngs` in `src/i18n/i18n.ts`
4. Add to language selector dropdown with native name (e.g., "Deutsch", "Espanol", "Francais")
5. Run `check-parity.ts` and `check-placeholders.ts` — must pass
6. Assign a native speaker reviewer before shipping

## CLDR plural categories by locale

<!-- markdownlint-disable MD060 -->

| Locale                          | Required categories                           |
| ------------------------------- | --------------------------------------------- |
| `en`, `de`, `es`, `fr`, `pt-br` | `one`, `other`                                |
| `ja`                            | `other` only (no singular/plural distinction) |
| `ar`                            | `zero`, `one`, `two`, `few`, `many`, `other`  |
| `pl`                            | `one`, `few`, `many`, `other`                 |
| `ru`                            | `one`, `few`, `many`, `other`                 |

<!-- markdownlint-enable MD060 -->

When generating translations for a locale, ensure every pluralized key (`_one`, `_other` in EN) produces all categories required by the target locale.
