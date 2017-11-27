module.exports = {
  useBabelInstrumenter: true,
  babelPlugins: [
    'babel-plugin-transform-es2015-destructuring',
    'babel-plugin-transform-es2015-parameters',
    'babel-plugin-transform-async-to-generator',
    'babel-plugin-transform-object-rest-spread',
    'babel-plugin-transform-class-properties'
  ],
  includeTranspiledSources: ['ts'],
  reporters: ['lcov', 'html', 'json'],
  excludes: ['*/mirage/**/*', '*/tests/**/*', '*/config/**/*', '*/public/**/*', '*/vendor/**/*', '*/app/actions/**']
};
