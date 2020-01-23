// Types for compiled templates
declare module '<%= destination %>/<%= dasherizedModuleName %>/templates/*' {
  import { TemplateFactory } from 'htmlbars-inline-precompile';
  const tmpl: TemplateFactory;
  export default tmpl;
}
