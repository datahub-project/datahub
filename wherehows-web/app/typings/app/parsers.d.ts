/**
 * A compiled grammar (.ne file) will output a js with compiled rules
 */
declare module 'wherehows-web/parsers/*' {
  import { CompiledRules } from 'nearley';
  const rules: CompiledRules;
  export default rules;
}
