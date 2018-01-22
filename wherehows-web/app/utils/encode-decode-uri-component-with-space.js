/**
 * Spaces should replaced with `+` to conform with application/x-www-form-urlencoded encoding
 *   https://url.spec.whatwg.org/#concept-urlencoded
 *   https://www.w3.org/TR/REC-html40/interact/forms.html#form-content-type
 * @param {String} sequence to be encoded
 * @returns {String} the encoded URI component sequence with spaces represented by plus
 */
export const encode = sequence => encodeURIComponent(sequence).replace(/\%20/g, '+');

/**
 * Decodes a URI component sequence by replacing `+` with spaces prior to decoding
 * https://www.w3.org/TR/REC-html40/interact/forms.html#form-content-type
 * @param {String} sequence to be decoded
 * @returns {String} the decoded URI component sequence with spaces
 */
export const decode = sequence => decodeURIComponent(String(sequence).replace(/\+/g, ' '));

/**
 *
 * @param string
 * @returns {string}
 */
export const deEntityIfy = string =>
  String(string).replace(/&([^&;]+);/g, (entity, sequence) => {
    const character = {
      lt: '<',
      gt: '>',
      quot: '"'
    }[sequence];

    return typeof character === 'string' ? character : entity;
  });

export const entityIfy = string =>
  String(string).replace(
    /["&<>]/g,
    c =>
      ({
        '"': '&quot;',
        '<': '&lt;',
        '>': '&gt;',
        '&': '&amp;'
      }[c])
  );
