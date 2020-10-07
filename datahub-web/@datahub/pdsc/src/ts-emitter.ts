/**
 * Format the value as a string constant.
 *
 * @param value to format
 */
export function stringConstant(value: string): string {
  return "'" + value.replace(/\n/g, ' ').replace(/\'/g, "\\'") + "'";
}

/**
 * Format the lines in the string as single line comments (//).
 *
 * @param docs string to format
 * @param prefix optional prefix for each line
 */
export function formatDocs(docs: string, prefix?: string): string {
  const start = prefix ? prefix + '//' : '//';
  return docs
    .split('\n')
    .map(line => start + (line === '' || line.startsWith(' ') ? line : ' ' + line))
    .join('\n');
}

/**
 * Capitalize the first letter of the word.
 *
 * @param word
 */
export function capitalize(word: string): string {
  if (!word || word.length === 0) {
    return word;
  }
  return word[0].toUpperCase() + word.substr(1);
}

/*
 * Make the url relative. this is needed to add the baseUrl at consumption.
 *
 * @param url
 */
export function makeRelative(url: string): string {
  if (!url || url.length === 0) {
    return url;
  }
  if (url.startsWith('/')) {
    return url.substr(1);
  }
  return url;
}

/**
 * Build a regex to remove the ignored package from start of string.
 */
export function buildIgnoreRegExp(ignorePackages: string[] | undefined): RegExp[] | undefined {
  return ignorePackages ? ignorePackages.map(a => new RegExp('^' + a)) : undefined;
}

/**
 * Clean the (fully qualified) type/namespace by removing any ignored packages from the start.
 *
 * @param _type
 * @param ignorePackages
 */
function cleanType(_type: string, ignorePackages: RegExp[] | undefined): string {
  // Eliminate any ignored package.
  let clean = _type;
  for (const re of ignorePackages || []) {
    clean = clean.replace(re, '');
  }
  return clean;
}

/**
 * Build the Typescript namespace from the java package name.
 *
 * @param java
 * @param ignorePackage optional regular expression to remove ignored package.
 */
export function packageToNamespace(_package: string, ignorePackages: RegExp[] | undefined): string[] {
  // Build a regex to remove the ignored package from start of string.
  const cleanNamespace = cleanType(_package, ignorePackages);
  const parts = cleanNamespace.split('.');
  const capitalized = parts.map(capitalize);
  return capitalized;
}

/**
 * Build the typescript fully qualified type for the full java type.
 *
 * @param _type package.class
 * @param ignorePackage regexp to use for eliminating ignored packages.
 */
export function typeToNamespace(_type: string, ignorePackages: RegExp[] | undefined, context?: string): string {
  // Translate all Urn types to string.
  if (_type.endsWith('Urn')) {
    return 'string';
  }

  // if there is a context and it is not in the package, add package to name
  const typeWithPackage = context && _type.indexOf('.') < 0 ? `${context}.${_type}` : _type;

  // Eliminate any ignored package.
  const clean = cleanType(typeWithPackage, ignorePackages);

  const parts = clean.split('.');
  const last = parts.pop();
  const capitalized = parts.map(capitalize);
  if (last) {
    capitalized.push(last);
  }
  return capitalized.join('.');
}
