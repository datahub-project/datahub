import slackLogo from '../assets/slacklogo.png?url';
import teamsLogo from '../assets/teamslogo.png?url';
import datahubLogo from '../assets/datahub-logo-color-stable.svg?url';

interface OriginLogoProps {
  originType: 'DATAHUB_UI' | 'SLACK' | 'TEAMS' | 'INGESTION_UI';
  size?: number;
}

const ORIGIN_CONFIGS: Record<string, { logo: string; alt: string }> = {
  SLACK: {
    logo: slackLogo,
    alt: 'Slack',
  },
  TEAMS: {
    logo: teamsLogo,
    alt: 'Microsoft Teams',
  },
  DATAHUB_UI: {
    logo: datahubLogo,
    alt: 'DataHub',
  },
  INGESTION_UI: {
    logo: datahubLogo,
    alt: 'DataHub Ingestion',
  },
};

export function OriginLogo({ originType, size = 20 }: OriginLogoProps) {
  const config = ORIGIN_CONFIGS[originType];

  if (!config) {
    return <span>?</span>;
  }

  return (
    <img
      src={config.logo}
      alt={config.alt}
      title={config.alt}
      className="origin-logo"
      style={{
        width: `${size}px`,
        height: `${size}px`,
        objectFit: 'contain',
      }}
    />
  );
}
