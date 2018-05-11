import { IDataset } from 'wherehows-web/typings/api/datasets/dataset';

const contentLimit = 500;

interface ISearchDataset extends IDataset {
  originalSchema?: string;
}

/**
 * Refactored from legacy code. This function's purpose is to modify the given Dataset Data in place
 * and shorten the content of its schema to a specified limit number of characters as well as ensure the
 * keyword searched for appears in the preview for the schema in the search entry
 * @param {Array<IDataset>} datum - given dataset data
 * @param keyword - keyword
 */
const createSearchEntry = (datum: ISearchDataset, keyword: string): void => {
  const content = datum.schema;

  if (content === undefined) {
    return;
  }

  const contentLength = content.length;

  if (keyword) {
    keyword = keyword.replace(/[+-]/g, '');
  }

  const schemaKeywordIdx = content.indexOf(keyword);

  let newContent: string = content;

  if (contentLength > contentLimit) {
    // We create a substring to shorten the content, either taking the whole string if it's short enough, or taking
    // a piece of the string starting with the keyword the user searched for
    newContent =
      contentLength - 1 < contentLimit
        ? content.substring(contentLength - contentLimit, contentLength)
        : content.substring(schemaKeywordIdx, contentLimit + schemaKeywordIdx);
  }
  // TODO: originalSchema may not be needed, but it appears to be a property accessed by a number of legacy functions
  datum.originalSchema = content;
  datum.schema = newContent;
};

/**
 * Refactored from legacy code. This utility function's purpose is to modify the dataset response for all datasets
 * resulting from a search. It's purpose is to shorten the "search preview" content on the page and bring attention
 * to the keyword the user used in the schema
 * @param {Array<IDataset>} data - given dataset data
 * @param {string} keyword - keyword
 */
export default function datasetsCreateSearchEntries(data: Array<ISearchDataset>, keyword: string) {
  data.forEach(datum => createSearchEntry(datum, keyword));
}
