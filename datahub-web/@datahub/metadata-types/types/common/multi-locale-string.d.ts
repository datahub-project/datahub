import { ILocale } from '@datahub/metadata-types/types/common/locale';

/**
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=MultiLocaleString.pdsc&path=models%2Fmodels%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2Fcommon&reponame=netrepo%2Fmodels
 *
 * Represents a textual field with values for multiple locales. Most readers should use the 'localized' field entry keyed by preferredLocale.
 */
export interface IMultiLocaleString {
  // Maps a locale to a localized version of the string. Each key is a Locale record converted to string format, with the language, country and variant separated by underscores. Examples: 'en', 'de_DE', 'en_US_WIN', 'de__POSIX', 'fr__MAC'.
  localized: Record<ILocale['language'], string>;
  // The preferred locale to use, based on standard rules
  preferredLocale: ILocale;
}
