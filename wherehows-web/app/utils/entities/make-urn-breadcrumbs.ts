import { datasetUrnRegexWH } from 'wherehows-web/utils/validators/urn';

interface IBreadCrumb {
  crumb: string;
  urn: string;
}

/**
 * Takes a urn string and parse it into an array of breadcrumb objects with crumb, and urn as
 * properties.
 * Hierarchy is implied in element ordering
 * @param {String} urn
 * @return {Array.<{crumb, urn}>|null}
 */
export default (urn: string): Array<{ crumb: string; urn: string }> | null => {
  const urnMatch = datasetUrnRegexWH.exec(urn);

  if (urnMatch) {
    // Initial element in a match array from RegExp#exec is the full match, not needed here
    const urnParts = urnMatch.filter((_match, index) => index);
    // Splits the 2nd captured group into an array of urn names and spreads into a new list
    const crumbs = [urnParts[0], ...urnParts[1].split('/')];

    // Reduces the crumbs into a list of crumb names and urn paths
    return crumbs.reduce(
      (breadcrumbs, crumb, index) => {
        const previousCrumb = breadcrumbs[index - 1];
        const breadcrumb: IBreadCrumb = {
          crumb,
          // First item is root
          urn: !index ? `${crumb}:///` : `${previousCrumb.urn}${crumb}/`
        };

        return [...breadcrumbs, breadcrumb];
      },
      <Array<IBreadCrumb>>[]
    );
  }

  return null;
};
