/**
 * Generates an absolute URL from a passed in relative url. If url is already absolute,
 * same url is returned. An IIFE is used to ensure that multiple invocations of the function
 * do not create multiple anchor elements that will need to be garbage collected
 * @export
 * @type {(url: string) => string}
 */
export const getAbsoluteUrl: (url: string) => string = ((): ((url: string) => string) => {
  let anchor: HTMLAnchorElement;

  /**
   * Returned closure function extracts the absolute url from the anchor element
   * in the closure.
   * @param {string} url
   * @returns {string}
   */
  return (url: string): string => {
    if (!anchor) {
      anchor = document.createElement('a');
    }
    anchor.href = url;

    return anchor.href;
  };
})();
