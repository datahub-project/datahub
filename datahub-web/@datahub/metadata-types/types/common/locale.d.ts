/**
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=Locale.pdsc&path=models%2Fmodels%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2Fcommon&reponame=netrepo%2Fmodels#Locale
 */
export interface ILocale {
  // A lowercase two-letter language code as defined by ISO-639.
  language: string;
  // An uppercase two-letter country code as defined by ISO-3166.
  country?: string;
  // Vendor or browser-specific code.
  variant?: string;
}
