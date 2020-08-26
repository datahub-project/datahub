import Component from '@glimmer/component';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { action } from '@ember/object';
import HealthProxy from '@datahub/shared/utils/health/health-proxy';
import { noop } from 'lodash';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';

interface IHealthHealthFactorsArgs {
  // Reference to the Health metadata for the related entity
  health?: Com.Linkedin.Common.Health;
  // Reference to the container action handler for Health validation call-to-action
  onHealthFactorAction?: (factor: Com.Linkedin.Common.HealthValidation) => void;
  // The Data model entity instance used to determine the behavior of factor actions
  entity?: DataModelEntityInstance;
  // Provides a link to the external resource for factor actions
  factorActionWiki?: string;
  // Action handler to perform a requested recalculation of the health score
  onRecalculateHealthScore?: () => void;
}

// Base class for the HealthFactors component
export const baseClass = 'health-factors';
// HealthFactors is represented as a Table, this is the block selector for each column
const columnClass = `${baseClass}-column`;

export default class HealthHealthFactors extends Component<IHealthHealthFactorsArgs> {
  /**
   * Reference to the baseClass for template access
   */
  baseClass = baseClass;

  /**
   * A proxy class for transformed health metadata attributes
   * @readonly
   */
  get healthProxy(): HealthProxy {
    return new HealthProxy(this.args.health);
  }

  /**
   * Configuration values for the NachoTable component
   * @readonly
   */
  get tableConfigs(): INachoTableConfigs<Com.Linkedin.Common.HealthValidation> {
    return {
      labels: ['validator', 'score', 'weight', 'description', 'factorAction'],
      headers: [
        { title: 'Factor', className: `${columnClass}__factor` },
        { title: 'Score', className: `${columnClass}__score` },
        { title: 'Impact on Health Score', className: `${columnClass}__impact` },
        { title: 'Description', className: `${columnClass}__description` },
        { title: '' }
      ],
      useBlocks: { header: false, body: false },
      customColumns: {
        score: {
          component: 'health/health-factors-score'
        },
        factorAction: {
          component: 'health/health-factor-action',
          componentAction: this.onHealthFactorAction
        }
      },
      options: {
        entity: this.args.entity,
        wikiLink: this.args.factorActionWiki
      }
    } as INachoTableConfigs<Com.Linkedin.Common.HealthValidation>;
  }

  /**
   * Handler to invoke the external action for Health Factor / validator cta
   * @param {Com.Linkedin.Common.HealthValidation} factor the Health validator for which the cta is being taken
   */
  @action
  onHealthFactorAction(factor: Com.Linkedin.Common.HealthValidation): void {
    const onHealthFactorAction = this.args.onHealthFactorAction || noop;
    onHealthFactorAction(factor);
  }

  /**
   * Handler invokes the external function to recalculate the associated health score
   */
  @action
  onRecalculateHealthScore(): void {
    const { onRecalculateHealthScore = noop } = this.args;
    onRecalculateHealthScore();
  }
}
