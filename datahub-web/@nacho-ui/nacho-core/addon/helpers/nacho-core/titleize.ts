import { helper } from '@ember/component/helper';
import utilTitleize, { ITitleizeOptions } from '@nacho-ui/core/utils/strings/titleize';

/**
 * Template helper version of the util function provided by nacho core that helps clean up property names
 * that are written in camel or dasherized case and returns something more clean and user friendly
 * @param value - template value to be titlelized
 * @param options - options given to the helper to pass into titleize function
 */
export function titleize([value, options]: [string, ITitleizeOptions]): string {
  return utilTitleize(value, options);
}

export default helper(titleize);
