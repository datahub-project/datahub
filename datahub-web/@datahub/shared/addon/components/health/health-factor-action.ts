import Component from '@glimmer/component';
import { NachoTableCustomColumnConfig, INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { Dictionary } from 'lodash';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { getDatasetUrnParts } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * Attributes supplied via the Nacho Table Row component
 * @interface IHealthHealthFactorActionArgs
 */
interface IHealthHealthFactorActionArgs {
  // Expected row level information for the Health validation
  rowData?: Com.Linkedin.Common.HealthValidation;
  // NachoTable configuration options for the row label
  labelConfig?: NachoTableCustomColumnConfig<Com.Linkedin.Common.HealthValidation>;
  // Modified table configuration options passed in to each row from Nacho Table
  tableConfigs?: INachoTableConfigs<Com.Linkedin.Common.HealthValidation> & {
    options?: { entity: DataModelEntityInstance; wikiLink: string };
  };
}

/**
 * Validator CTA attributes to be passed down to the Validator record in a HealthFactorAction
 * @interface IHealthValidatorCta
 */
interface IHealthValidatorCta {
  // CTA text to be shown in the button
  text: string;
  // Flag indicating that this CTA is an external wiki link
  isWiki?: boolean;
}

/**
 * Handles CTA button for each row in the Health Factors table
 * @export
 * @class HealthHealthFactorAction
 * @extends {Component<IHealthHealthFactorActionArgs>}
 */
export default class HealthHealthFactorAction extends Component<IHealthHealthFactorActionArgs> {
  /**
   * Specifies the control name for tracking action interactions (clicks) based on the current
   * validator
   * @readonly
   */
  get controlName(): string {
    const validator = this.args.rowData?.validator || '';
    const controlNames: Dictionary<string> = {
      Ownership: 'DataHubHealthScoreFactorsActionViewOwnership',
      Description: 'DataHubHealthScoreFactorsActionViewDescription'
    };

    return controlNames[validator];
  }

  /**
   * Provides the CTA text for the available validators for Health metadata
   * @readonly
   */
  get cta(): IHealthValidatorCta | undefined {
    const validator = this.args.rowData?.validator || '';
    const knownValidatorCtas: Dictionary<IHealthValidatorCta> = {
      Ownership: { text: 'View Owners', isWiki: this.validatorHasExternalAction },
      Description: { text: 'See Details in Wiki', isWiki: true }
    };

    return knownValidatorCtas[validator];
  }

  /**
   * Determines if the cta for the related validator is a link to the external resource
   * Temporary implementation, will be derived from a config endpoint that responds with
   * a shape similar to:
   *   {
   *     [validator]: {
   *       text: String;
   *       link?: String;
   *       inPageRedirectComponentName?: enum /string
   *     }
   *   }
   * @TODO: META-11944 Integrate Health Validator config endpoint
   * @readonly
   */
  get validatorHasExternalAction(): boolean {
    const entity = this.args.tableConfigs?.options?.entity;

    if (entity) {
      // Attempt to parse the urn for a platform to check if it matches the expected platform
      // for external action or if the related entity is a metric entity
      const { platform } = getDatasetUrnParts(entity.urn);
      let isExternal = entity.displayName === 'metrics';

      if (platform) {
        // Short circuit if already truthy
        isExternal = isExternal || platform === DatasetPlatform.UMP;
      }

      return isExternal;
    }

    return false;
  }
}
