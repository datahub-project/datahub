import lookerLogo from '../../../images/lookerlogo.png';
import hdfsLogo from '../../../images/hadooplogo.png';
import kafkaLogo from '../../../images/kafkalogo.png';
import hiveLogo from '../../../images/hivelogo.png';

/**
 * TODO: This is a temporary solution, until the backend can push logos for all data platform types.
 */
export function getLogoFromPlatform(platform: string) {
    if (platform === 'Looker') {
        return lookerLogo;
    }
    if (platform === 'hdfs') {
        return hdfsLogo;
    }
    if (platform === 'kafka') {
        return kafkaLogo;
    }
    if (platform === 'hive') {
        return hiveLogo;
    }
    return undefined;
}
