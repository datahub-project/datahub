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

const deEntityIfyMap: Record<string, string> = {
  lt: '<',
  gt: '>',
  quot: '"'
};
/**
 *
 * @param str
 * @returns {string}
 */
export const deEntityIfy = (str: string): string =>
  String(str).replace(/&([^&;]+);/g, (entity, sequence): string => {
    const character: string | undefined = deEntityIfyMap[sequence];

    return typeof character === 'string' ? character : entity;
  });

const entityIfyMap: Record<string, string> = {
  '"': '&quot;',
  '<': '&lt;',
  '>': '&gt;',
  '&': '&amp;'
};

export const entityIfy = (str: string): string => String(str).replace(/["&<>]/g, (c): string => entityIfyMap[c]);
