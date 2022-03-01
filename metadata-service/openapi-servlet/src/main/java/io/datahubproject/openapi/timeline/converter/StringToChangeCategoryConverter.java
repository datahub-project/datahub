package io.datahubproject.openapi.timeline.converter;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;

import static com.linkedin.metadata.timeline.data.ChangeCategory.*;


public class StringToChangeCategoryConverter implements Converter<String, ChangeCategory> {
  @Override
  public ChangeCategory convert(String source) {
    try {
      String upperCase = source.toUpperCase();
      // For compound enums, want to support different cases i.e. technical_schema, technical schema, technical-schema, etc.
      Optional<ChangeCategory> compoundCategory = COMPOUND_CATEGORIES.keySet().stream()
          .filter(compoundCategoryKey -> matchCompound(compoundCategoryKey, upperCase))
          .map(COMPOUND_CATEGORIES::get)
          .findFirst();
      return compoundCategory.orElseGet(() -> ChangeCategory.valueOf(upperCase));
    } catch (Exception e) {
      throw new ConversionFailedException(TypeDescriptor.valueOf(String.class),
          TypeDescriptor.valueOf(ChangeCategory.class), source, e);
    }
  }

  private boolean matchCompound(@Nonnull List<String> compoundCategoryKey, @Nonnull String source) {
    return compoundCategoryKey.stream()
        .allMatch(source::contains);
  }
}
