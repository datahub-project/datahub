# Imports should not containt /addon/ (no-addon-import)

On the open source world we should not import files that are internal. This will help us correct that behavior.

## Rule Details

Examples of **incorrect** code for this rule:

```js
import something from '@linkedin/something';

```

Examples of **correct** code for this rule:

```js
import something from '@datahub/something';

```

## When Not To Use It

This rule should only be applied on open source files
