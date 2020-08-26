import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { IFeatureEntity } from '@datahub/metadata-types/types/entity/feature/feature-entity';
import { computed } from '@ember/object';
import {
  readFeature,
  updateFeatureEditableConfig,
  updateFeatureStatusConfig
} from '@datahub/data-models/api/feature/feature';
import { IFeatureSnapshot } from '@datahub/metadata-types/types/metadata/feature-snapshot';
import { readFeatureSnapshot, readFeatureSnapshots } from '@datahub/data-models/api/feature/snapshot';
import { getMetadataAspect } from '@datahub/metadata-types/constants/metadata/aspect';
import { IFeatureAspect } from '@datahub/metadata-types/types/entity/feature/feature-aspect';
import { IAvailabilityInfo } from '@datahub/metadata-types/types/entity/feature/frame/availability-info';
import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';
import { AvailabilityEnvironmentType } from '@datahub/metadata-types/constants/entity/feature/frame/availability-environment-type';
import { capitalize } from 'lodash';
import { IFeatureMultiProductInfo } from '@datahub/metadata-types/types/entity/feature/frame/feature-multiproduct-info';
import { alias, map, equal, oneWay } from '@ember/object/computed';
import { IFrameAnchorConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-anchor-config';
import { getRenderProps } from '@datahub/data-models/entity/feature/render-props';
import { IFrameFeatureEditableConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-editable-config';
import { IFrameFeatureTierConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-tier-config';
import { IFrameFeatureStatusConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-status-config';
import { IFrameFeatureConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-config';
import { ISearchFeatureEntity } from '@datahub/data-models/types/entity/feature/features';
import { getCategoryPathString } from '@datahub/data-models/entity/feature/utils';
import { relationship } from '@datahub/data-models/relationships/decorator';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * possible types for the entity
 */
type FeatureEntityType = IFeatureEntity | ISearchFeatureEntity;

/**
 * Defines the entity data model for the Feature entity type
 *
 * FeatureEntity inherits and extends the BaseEntity class by adding/overriding/hiding properties
 * and methods on the static, IBaseEntityStatics and instance, BaseEntity interfaces over the entity
 * interface IFeatureEntity
 * @export
 * @class FeatureEntity
 * @extends {BaseEntity}
 */
@statics<IBaseEntityStatics<FeatureEntityType>>()
export class FeatureEntity extends BaseEntity<FeatureEntityType> {
  /**
   * TODO META-10097: This should not be needed as it is specified in super class
   * Reference to the underlying feature entity
   * @type {IFeatureEntity}
   * @memberof FeatureEntity
   */
  entity?: IFeatureEntity;

  /**
   * TODO META-10097: We should be consistent with entity and use a generic type on the super class
   * Specifies that snapshot attribute is subtype of Snapshot intersection type, IFeatureSnapshot
   * @type {IFeatureSnapshot}
   * @memberof FeatureEntity
   */
  snapshot?: IFeatureSnapshot;

  /**
   * Discriminant for Entity type
   * @static
   * @memberof FeatureEntity
   */
  static kind = 'FeatureEntity';

  /**
   * Discriminant when included in a tagged Entity union
   */
  get kind(): string {
    return FeatureEntity.kind;
  }

  /**
   * The human friendly alias for Feature entities
   * Statically accessible on the FeatureEntity type
   * @static
   * @memberof FeatureEntity
   */
  static displayName: 'ml-features' = 'ml-features';

  /**
   * The human friendly alias for Feature entities
   */
  get displayName(): 'ml-features' {
    return FeatureEntity.displayName;
  }

  /**
   * Class properties common across instances
   * Dictates how visual ui components should be rendered
   * Implemented as a getter to ensure that reads are idempotent
   * @readonly
   * @static
   * @type {IEntityRenderProps}
   */
  static get renderProps(): IEntityRenderProps {
    return getRenderProps();
  }

  /**
   * Reads the snapshots for a list of Feature urns
   * @static
   */
  static readSnapshots(urns: Array<string>): Promise<Array<IFeatureSnapshot>> {
    return readFeatureSnapshots(urns);
  }

  /**
   * Updates this Features IFrameFeatureEditableConfig metadata aspect with the supplied argument editableConfig
   * @param {IFrameFeatureEditableConfig} editableConfig the editable config aspect to update this Feature with
   * @memberof FeatureEntity
   */
  updateEditableConfig(editableConfig: IFrameFeatureEditableConfig): ReturnType<typeof updateFeatureEditableConfig> {
    return updateFeatureEditableConfig(this.urn, editableConfig);
  }

  /**
   * Updates this Features IFrameFeatureStatusConfig metadata aspect with the supplied argument statusConfig
   * @param {IFrameFeatureStatusConfig} statusConfig the status config aspect to update this Feature with
   * @memberof FeatureEntity
   */
  updateStatusConfig(statusConfig: IFrameFeatureStatusConfig): ReturnType<typeof updateFeatureStatusConfig> {
    return updateFeatureStatusConfig(this.urn, statusConfig);
  }

  /**
   * Retrieves the value of the IFeatureEntity identified by this.urn
   * Computed macro allows us to cache value on access
   * @readonly
   * @type {Promise<IFeatureEntity>}
   * @memberof FeatureEntity
   */
  @computed()
  get readEntity(): Promise<IFeatureEntity> {
    return readFeature(this.urn);
  }

  /**
   * Retrieves the current Snapshot for this Entity
   * @readonly
   * @type {Promise<IFeatureSnapshot>}
   * @memberof FeatureEntity
   */
  @computed()
  get readSnapshot(): Promise<IFeatureSnapshot> {
    return readFeatureSnapshot(this.urn);
  }

  /**
   * Private proxy function for configuration aspects on the Feature snapshot
   * @readonly
   * @private
   * @type {{
   *     editableConfig: IFeatureAspect['com.linkedin.feature.frame.FrameFeatureEditableConfig'];
   *     featureConfig: IFeatureAspect['com.linkedin.feature.frame.FrameFeatureConfig'];
   *   }}
   * @memberof FeatureEntity
   */
  @computed('snapshot')
  get featureConfigs(): {
    editableConfig: IFeatureAspect['com.linkedin.feature.frame.FrameFeatureEditableConfig'];
    featureConfig: IFeatureAspect['com.linkedin.feature.frame.FrameFeatureConfig'];
    tierConfig: IFeatureAspect['com.linkedin.feature.frame.FrameFeatureTierConfig'];
  } {
    const { snapshot } = this;

    // Editable config contains the snapshot attributes that the user is able to modify
    // These should be shown first before other attributes below i.e. order of operations is desired
    const editableConfig = getMetadataAspect(snapshot)(
      'com.linkedin.feature.frame.FrameFeatureEditableConfig'
    ) as IFeatureAspect['com.linkedin.feature.frame.FrameFeatureEditableConfig'];
    // Other config properties
    const featureConfig = getMetadataAspect(snapshot)(
      'com.linkedin.feature.frame.FrameFeatureConfig'
    ) as IFeatureAspect['com.linkedin.feature.frame.FrameFeatureConfig'];
    const tierConfig = getMetadataAspect(snapshot)(
      'com.linkedin.feature.frame.FrameFeatureTierConfig'
    ) as IFeatureAspect['com.linkedin.feature.frame.FrameFeatureTierConfig'];

    return {
      editableConfig,
      featureConfig,
      tierConfig
    };
  }

  /**
   * References the Feature multiproductInfo associated with the multiproduct in which the feature is defined
   * @type {IFeatureMultiProductInfo}
   * @memberof FeatureEntity
   */
  @alias('featureConfigs.featureConfig.multiproductInfo')
  multiproductInfo?: IFeatureMultiProductInfo;

  /**
   * Lists the urns for datasets related to the Feature instance extracted from the anchors on the FrameFeatureConfig metadata aspect
   */
  @map(
    'featureConfigs.featureConfig.anchors.[]',
    ({ source: { datasetUrn } }: IFrameAnchorConfig): string | undefined => datasetUrn
  )
  datasetUrns!: Array<string>;

  /**
   * Lists the hdfs source paths for the Feature Entity if present on the Feature snapshot anchors
   * @memberof FeatureEntity
   */
  @map('featureConfigs.featureConfig.anchors.[]', ({ source: { properties } }: IFrameAnchorConfig): string => {
    const hdfsProps = properties['com.linkedin.feature.frame.HDFSSourceProperties'];
    return hdfsProps ? hdfsProps.path : '';
  })
  hdfsPaths!: Array<string>;

  /**
   * Hoists a reference to the editableConfig as a top level reference
   * @memberof FeatureEntity
   */
  @alias('featureConfigs.editableConfig')
  editableConfig!: IFrameFeatureEditableConfig;

  /**
   * Hoists a reference to the featureConfig value as a top level reference
   * @memberof FeatureEntity
   */
  @alias('featureConfigs.featureConfig')
  featureConfig?: IFrameFeatureConfig;

  /**
   * Hoists a reference to the tierConfig value as a top level reference
   * @memberof FeatureEntity
   */
  @alias('featureConfigs.tierConfig')
  tierConfig?: IFrameFeatureTierConfig;

  /**
   * Creates relationship between this entity and its owner entities
   */
  @relationship('people', 'ownerEntityUrns')
  ownerEntities!: Array<PersonEntity>;

  /**
   * Frame owners for the Feature serialized as a string for ui rendering
   * @readonly
   * @type {string}
   * @memberof FeatureEntity
   */
  @computed('ownerUrns')
  get formattedFrameOwners(): string {
    /**
     * Serializes the Feature Owner instance as a list of LDAP user names if we have ownership info,
     * otherwise we return a placeholder
     * @param {Array<string>} [ownerUrns=[]]
     * @param {{ placeholder: string; prefix: RegExp }} { placeholder, prefix }
     * @returns {string}
     */
    const toStringAndRemoveUrnPrefix = (ownerUrns: Array<string> = [], { prefix }: { prefix: RegExp }): string => {
      const stringWithoutPrefix = ownerUrns.join(', ').replace(prefix, (): string => '');
      const featureEntityPageRenderProps = FeatureEntity.renderProps.entityPage;
      const attributePlaceholder =
        (featureEntityPageRenderProps && featureEntityPageRenderProps.attributePlaceholder) || '';

      return stringWithoutPrefix || attributePlaceholder;
    };

    return toStringAndRemoveUrnPrefix(this.ownerUrns, { prefix: /urn:li:corpuser:/g });
  }

  /**
   * Selects the list of environments where the feature or anchor is available
   * @readonly
   * @type {Array<AvailabilityEnvironmentType>}
   * @memberof FeatureEntity
   */
  @computed('snapshot')
  get dataAvailability(): Array<AvailabilityEnvironmentType> {
    const availabilityConfig = getMetadataAspect(this.snapshot)(
      'com.linkedin.feature.frame.FrameFeatureAvailabilityConfig'
    ) as IFeatureAspect['com.linkedin.feature.frame.FrameFeatureAvailabilityConfig'];

    return availabilityConfig
      ? availabilityConfig.availability.map(
          ({ environment }: IAvailabilityInfo): AvailabilityEnvironmentType => environment
        )
      : [];
  }

  /**
   * Serializes the list of environments where the feature or anchor is available
   * @readonly
   * @type {string}
   * @memberof FeatureEntity
   */
  @computed('dataAvailability')
  get formattedDataAvailability(): string {
    const featureEntityPageRenderProps = FeatureEntity.renderProps.entityPage;
    const placeholder = featureEntityPageRenderProps ? featureEntityPageRenderProps.attributePlaceholder : '';
    const [last = placeholder, ...preceding] = this.dataAvailability
      .map((availability, _, { length }): string =>
        // If we have only one availability option then append with only, otherwise just toggle the case
        length === 1 ? `${availability}-only` : availability
      )
      .reverse();

    // If we have 2 or more elements, separate the preceding with a comma and/or conjugate the last with an 'and',
    // otherwise show the single element or placeholder ('-')
    return capitalize(preceding.length ? `${preceding.join(', ')} and ${last}` : last);
  }

  /**
   * The current status of the feature,
   * it will try to read it from snapshot, will fallback to entity if not found
   * @readonly
   * @type {(FeatureStatusType | void)}
   * @memberof FeatureEntity
   */
  @computed('snapshot', 'entity.status')
  get featureStatus(): FeatureStatusType | void {
    const featureStatus = getMetadataAspect(this.snapshot)(
      'com.linkedin.feature.frame.FrameFeatureStatusConfig'
    ) as IFeatureAspect['com.linkedin.feature.frame.FrameFeatureStatusConfig'];
    const { entity } = this;

    return featureStatus ? featureStatus.status : entity ? entity.status : undefined;
  }

  /**
   * returns the path for this feature
   */
  @computed('baseEntity', 'category', 'classification')
  get categoryPathString(): string {
    return getCategoryPathString(this);
  }

  /**
   * Convenience flag indicating that the Feature has a FeatureStatusType.Published status
   * @memberof FeatureEntity
   */
  @equal('featureStatus', FeatureStatusType.Published)
  isPublished!: boolean;

  /**
   * Convenience flag for Feature in incomplete status
   * @memberof FeatureHeader
   */
  @equal('featureStatus', FeatureStatusType.Incomplete)
  isIncomplete!: boolean;

  /**
   * Convenience flag for Feature in unpublished status
   * @memberof FeatureHeader
   */
  @equal('featureStatus', FeatureStatusType.Unpublished)
  isUnpublished!: boolean;

  /**
   * Alias for search
   */
  @oneWay('entity.availability')
  availability!: ISearchFeatureEntity['availability'];

  /**
   * Alias for search results
   */
  @oneWay('entity.baseEntity')
  baseEntity!: ISearchFeatureEntity['baseEntity'];

  /**
   * Alias for search results
   */
  @oneWay('entity.category')
  category!: ISearchFeatureEntity['category'];

  /**
   * Alias for search results
   */
  @oneWay('entity.baseEntity')
  classification!: ISearchFeatureEntity['classification'];

  /**
   * Alias for search results
   */
  @oneWay('entity.description')
  description!: ISearchFeatureEntity['description'];

  /**
   * Alias for search results
   */
  @oneWay('entity.multiproduct')
  multiproduct!: ISearchFeatureEntity['multiproduct'];

  /**
   * Alias for search results
   */
  @oneWay('entity.name')
  name!: ISearchFeatureEntity['name'];

  /**
   * Alias for search results
   */
  @oneWay('entity.namespace')
  namespace!: ISearchFeatureEntity['namespace'];

  /**
   * Alias for search results
   */
  @oneWay('entity.tier')
  tier!: ISearchFeatureEntity['tier'];

  /**
   * Alias for search results
   */
  @oneWay('entity.owners')
  ownersSimple!: ISearchFeatureEntity['owners'];

  /**
   * Unifies list of owner urns whether available through readEntity() for fetched by search
   */
  @computed('ownersSimple', 'ownerUrns')
  get ownerEntityUrns(): Array<string> {
    const { ownersSimple, ownerUrns } = this;
    const urnFromOwnersSimple =
      ownersSimple && ownersSimple.map((username): string => PersonEntity.urnFromUsername(username));
    return urnFromOwnersSimple || ownerUrns || [];
  }
}
