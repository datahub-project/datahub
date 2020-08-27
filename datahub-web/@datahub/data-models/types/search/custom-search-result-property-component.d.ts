import { ICustomSearchResultPropertyComponentTag } from '@datahub/data-models/types/search/custom-search-result-property-component/tag';
import { ICustomSearchResultPropertyComponentLink } from '@datahub/data-models/types/search/custom-search-result-property-component/link';
import { ICustomSearchResultPropertyComponentDate } from '@datahub/data-models/types/search/custom-search-result-property-component/date';
import { ICustomSearchResultPropertyComponentIcon } from '@datahub/data-models/types/search/custom-search-result-property-component/icon';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

// TODO https://jira01.corp.linkedin.com:8443/browse/META-10764: Investigate how to solve the 'wrapper' components scalability issue in generic entity page and search

/**
 * This interface should live in search addon, but search addon depends on data-models and
 * we don't want to create a circular dependency. Storing this interface at data-models for now.
 *
 * Search can have custom components to be render on fields, tags or secondary actions.
 *
 * Unifying interface to invoke these components
 */

/**
 * Known components are the ones that are generic and reusable for all kind of purposes
 */
type KnownComponents =
  | ICustomSearchResultPropertyComponentTag
  | ICustomSearchResultPropertyComponentLink
  | ICustomSearchResultPropertyComponentDate
  | ICustomSearchResultPropertyComponentIcon;

/**
 * Making explicit this distintion to make it easier to add new KnownComponents
 */
export type ICustomSearchResultPropertyComponent = KnownComponents | IDynamicComponent;
