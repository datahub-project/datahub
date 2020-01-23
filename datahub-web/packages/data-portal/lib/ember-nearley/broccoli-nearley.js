const nearley = require('nearley');
const compile = require('nearley/lib/compile');
const generate = require('nearley/lib/generate');
const nearleyGrammar = require('nearley/lib/nearley-language-bootstrapped');
const Filter = require('broccoli-filter');

/**
 * Will transform .ne files into .js files with Grammar Compiled Rules
 * that are ready to consume by nearly parser.
 *
 * We are using Filter as a base Broccoli plugin
 */
class BroccoliNearley extends Filter {
  /**
   * Extending constructor to add '.ne' extensions
   * @param {*} inputNode
   */
  constructor(inputNode) {
    super(inputNode);

    this.extensions = ['ne'];
    this.targetExtension = 'js';
  }

  /**
   * Process a file: Will compile it and prepare it to be consumed.
   * @param {*} content File content with grammar
   * @param {*} relativePath path of the original file
   */
  processString(content, relativePath) {
    const code = this.compileGrammar(content);

    return `define('${relativePath.replace(/\.ne/, '')}', ['exports'], function(exports) {
      const module = { exports: {} };
      ${code}
      exports.default = module.exports;
    });`;
  }

  /**
   * Compile Grammar as specified in Nearley Docs.
   * @param {*} sourceCode content of ne file
   */
  compileGrammar(sourceCode) {
    // Parse the grammar source into an AST
    const grammarParser = new nearley.Parser(nearleyGrammar);
    grammarParser.feed(sourceCode);
    const grammarAst = grammarParser.results[0]; // TODO check for errors

    // Compile the AST into a set of rules
    const grammarInfoObject = compile(grammarAst, {});
    // Generate JavaScript code from the rules
    const grammarJs = generate(grammarInfoObject, 'grammar');

    return grammarJs;
  }
}

module.exports = BroccoliNearley;
