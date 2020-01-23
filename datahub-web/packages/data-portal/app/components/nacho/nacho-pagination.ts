import Component from '@ember/component';
import { computed } from '@ember/object';
import { tagName } from '@ember-decorators/component';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

/**
 * Query params with pages for routes
 */
interface IQueryParamsWithPages {
  page: number;
}

/**
 * Page structure to render pages
 */
interface IPage<T = unknown, P extends IQueryParamsWithPages = { page: number }> {
  pageNumber: number;
  isCurrent: boolean;
  isSeparator: boolean;
  link: IDynamicLinkNode<T, string, P>;
}

/**
 * Type alias to improve readability
 */
type IPageWithParams<T, P extends object> = IPage<T, P & IQueryParamsWithPages>;

/**
 * Type alias: Object with pages
 */
type WithPages<Z extends object> = Z & IQueryParamsWithPages;

/**
 * Util to add page param to query params in a link
 * @param linkTo
 * @param page
 */
const addPageToLink = <T, P, Z extends object>(
  linkTo: IDynamicLinkNode<T, P, Z>,
  page: number
): IDynamicLinkNode<T, P, WithPages<Z>> => {
  return {
    ...linkTo,
    queryParams: {
      ...((linkTo.queryParams as Z) || {}),
      page
    }
  };
};

/**
 * Nacho Pagination component:
 *
 * it will render pagination component. Parameters:
 *
 * currentPage: The page that is current
 * totalPages: Number of pages to show
 * linkTo: dynamic link type of link (https://github.com/asross/dynamic-link). This library will add
 *  page parameter automatically
 *
 * This component should be moved to its own addon
 */
@tagName('')
export default class NachoPagination<T, P extends object> extends Component {
  /**
   * Current page to render
   */
  currentPage: number;

  /**
   * Number of pages that are available
   */
  totalPages: number;

  /**
   * Number of pages to show before and after current page before we show ellipsis '...'.
   */
  threshold: number = 3;

  /**
   * Dynamic link to generate page links
   */
  linkTo: IDynamicLinkNode<T, string, P>;

  /**
   * Previous page number
   */
  @computed('currentPage')
  get previousPage(): number {
    const currentPage = this.currentPage;
    if (currentPage <= 1) {
      return currentPage;
    } else {
      return currentPage - 1;
    }
  }

  /**
   * Next page number
   */
  @computed('currentPage')
  get nextPage(): number {
    const currentPage = this.currentPage;
    const totalPages = this.totalPages;
    if (currentPage >= totalPages) {
      return totalPages;
    } else {
      return currentPage + 1;
    }
  }

  /**
   * If current page is first page
   */
  @computed('currentPage')
  get first(): boolean {
    const currentPage = this.currentPage;
    if (currentPage <= 1) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * If current page is last page
   */
  @computed('currentPage')
  get last(): boolean {
    const currentPage = this.currentPage;
    const totalPages = this.totalPages;
    if (currentPage >= totalPages) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Generate previous page link
   */
  @computed('linkTo', 'previousPage')
  get prevLink(): IDynamicLinkNode<T, string, WithPages<P>> {
    return addPageToLink(this.linkTo, this.previousPage);
  }

  /**
   * Generate next page link
   */
  @computed('linkTo', 'nextPage')
  get nextLink(): IDynamicLinkNode<T, string, WithPages<P>> {
    return addPageToLink(this.linkTo, this.nextPage);
  }

  /**
   * Will generate the array of pages to show this:
   * 1 ... 4 5 6 ... 9
   */
  @computed('currentPage', 'totalPages', 'linkTo')
  get pages(): Array<IPageWithParams<T, P>> {
    const { currentPage, totalPages, linkTo, threshold } = this;
    const pages: Array<IPageWithParams<T, P>> = [];
    const hasInitialSepartor = currentPage - threshold > 2;
    const hasLastSeparator = currentPage + threshold < totalPages - 1;
    const startLoop = hasInitialSepartor ? currentPage - threshold : 2;
    const endLoop = hasLastSeparator ? currentPage + threshold : totalPages - 1;

    const addPage = ({ pageNumber = -1, isSeparator = false }: Partial<IPage>): void => {
      pages.push({
        pageNumber,
        isSeparator,
        isCurrent: pageNumber === currentPage,
        link: addPageToLink(linkTo, pageNumber)
      });
    };

    // 1
    addPage({ pageNumber: 1 });
    // ...
    hasInitialSepartor && addPage({ isSeparator: true });
    // 4, 5, 6
    for (let i = startLoop; i <= endLoop; i += 1) {
      addPage({ pageNumber: i });
    }
    // ...
    hasLastSeparator && addPage({ isSeparator: true });
    // 8
    totalPages > 1 && addPage({ pageNumber: totalPages });
    return pages;
  }
}
