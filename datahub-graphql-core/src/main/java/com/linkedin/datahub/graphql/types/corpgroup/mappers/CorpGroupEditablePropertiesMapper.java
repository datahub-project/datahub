package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroupEditableProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpGroupEditablePropertiesMapper
    implements ModelMapper<
        com.linkedin.identity.CorpGroupEditableInfo, CorpGroupEditableProperties> {

  private final Logger _logger =
      LoggerFactory.getLogger(CorpGroupEditablePropertiesMapper.class.getName());

  public static final CorpGroupEditablePropertiesMapper INSTANCE =
      new CorpGroupEditablePropertiesMapper();

  public static CorpGroupEditableProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpGroupEditableInfo corpGroupEditableInfo) {
    return INSTANCE.apply(context, corpGroupEditableInfo);
  }

  @Override
  public CorpGroupEditableProperties apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpGroupEditableInfo corpGroupEditableInfo) {
    final CorpGroupEditableProperties result = new CorpGroupEditableProperties();
    result.setDescription(corpGroupEditableInfo.getDescription(GetMode.DEFAULT));
    result.setSlack(corpGroupEditableInfo.getSlack(GetMode.DEFAULT));
    result.setEmail(corpGroupEditableInfo.getEmail(GetMode.DEFAULT));
    com.linkedin.common.url.Url pictureLinkObject =
        corpGroupEditableInfo.getPictureLink(GetMode.NULL);
    String pictureLink = null;
    if (pictureLinkObject != null) {
      pictureLink = pictureLinkObject.toString();
    }
    result.setPictureLink(pictureLink);
    return result;
  }
}
