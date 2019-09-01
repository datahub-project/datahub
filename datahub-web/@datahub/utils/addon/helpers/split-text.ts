import { helper } from '@ember/component/helper';

export function splitText([string = '', maxLen = 20, separator = '...']: [string, number?, string?]): string {
  const { length: strLen } = string;
  const { length: sepLen } = separator;

  if (strLen > maxLen) {
    const splitLen = Math.floor((maxLen - sepLen) / 2);

    return `${string.substr(0, splitLen).trim()}${separator}${string.substr(-splitLen).trim()}`;
  }

  return string;
}

export default helper(splitText);
