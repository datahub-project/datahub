/**
 * Spaces should replaced with `+` to conform with application/x-www-form-urlencoded encoding
 *   https://url.spec.whatwg.org/#concept-urlencoded
 *   https://www.w3.org/TR/REC-html40/interact/forms.html#form-content-type
 * @param {string} sequence to be encoded
 * @returns {string} the encoded URI component sequence with spaces represented by plus
 */
export const encode = (sequence: string): string => encodeURIComponent(sequence).replace(/%20/g, '+');

/**
 * Decodes a URI component sequence by replacing `+` with spaces prior to decoding
 * https://www.w3.org/TR/REC-html40/interact/forms.html#form-content-type
 * @param {string} sequence to be decoded
 * @returns {string} the decoded URI component sequence with spaces
 */
export const decode = (sequence: string): string => decodeURIComponent(String(sequence).replace(/\+/g, ' '));

/**
 *
 * @param str
 * @returns {string}
 */
export const deEntityIfy = (str: string) =>
  String(str).replace(/&([^&;]+);/g, (entity, sequence) => {
    const character = (<{ [s: string]: string }>{
      lt: '<',
      gt: '>',
      quot: '"'
    })[sequence];

    return typeof character === 'string' ? character : entity;
  });

export const entityIfy = (str: string): string =>
  String(str).replace(
    /["&<>]/g,
    c =>
      (<{ [s: string]: string }>{
        '"': '&quot;',
        '<': '&lt;',
        '>': '&gt;',
        '&': '&amp;'
      })[c]
  );
