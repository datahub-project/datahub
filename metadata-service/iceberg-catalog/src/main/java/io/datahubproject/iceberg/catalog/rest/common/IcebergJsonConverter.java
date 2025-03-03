package io.datahubproject.iceberg.catalog.rest.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import javax.annotation.Nonnull;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

public class IcebergJsonConverter extends MappingJackson2HttpMessageConverter {
  private static final String ICEBERG_PACKAGE_PREFIX = "org.apache.iceberg.";

  public IcebergJsonConverter(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  protected boolean supports(@Nonnull Class<?> clazz) {
    return isClassInPackage(clazz);
  }

  @Override
  public boolean canRead(@Nonnull Type type, Class<?> contextClass, MediaType mediaType) {
    return hasTypeInPackage(type) && super.canRead(type, contextClass, mediaType);
  }

  @Override
  public boolean canWrite(@Nonnull Class<?> clazz, MediaType mediaType) {
    return isClassInPackage(clazz) && super.canWrite(clazz, mediaType);
  }

  private boolean hasTypeInPackage(Type type) {
    if (type instanceof Class<?>) {
      return isClassInPackage((Class<?>) type);
    }

    if (type instanceof ParameterizedType) {
      ParameterizedType paramType = (ParameterizedType) type;

      // Check raw type
      Type rawType = paramType.getRawType();
      if (rawType instanceof Class<?> && isClassInPackage((Class<?>) rawType)) {
        return true;
      }

      // Recursively check type arguments
      for (Type typeArg : paramType.getActualTypeArguments()) {
        if (hasTypeInPackage(typeArg)) {
          return true;
        }
      }
    }

    if (type instanceof WildcardType) {
      WildcardType wildcardType = (WildcardType) type;
      // Check upper bounds
      for (Type bound : wildcardType.getUpperBounds()) {
        if (hasTypeInPackage(bound)) {
          return true;
        }
      }
      // Check lower bounds
      for (Type bound : wildcardType.getLowerBounds()) {
        if (hasTypeInPackage(bound)) {
          return true;
        }
      }
    }

    if (type instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) type;
      return hasTypeInPackage(arrayType.getGenericComponentType());
    }

    return false;
  }

  private static boolean isClassInPackage(@Nonnull Class<?> clazz) {
    return clazz.getName().startsWith(ICEBERG_PACKAGE_PREFIX);
  }
}
