# Imports should not containt /addon/ (no-addon-import)

VScode usually autocompletes the import with `/addon/` which is invalid in ember
and will yield some errors. There are some cases where the import only affect TS (types)
and then it will compile fine, but we want to mantain consistency across code, therefore,
having `/addon/` importing ember addons should not be allowed.

## Rule Details

Examples of **incorrect** code for this rule:

```js
import something from 'someemberaddon/addon/component/something';

```

Examples of **correct** code for this rule:

```js
import something from 'someemberaddon/component/something';

```

## When Not To Use It

If you are having conflics with naming and you really want to import `/addon/`
