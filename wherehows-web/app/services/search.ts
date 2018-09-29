import Service from '@ember/service';

/**
 * Search service is used to maintain the same
 * state on different parts of the app. Right now the only thing
 * we need to persist is the keywords.
 */
export default class Search extends Service {
  keyword: string;
}
