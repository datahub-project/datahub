package io.datahubproject.schematron.models;

import com.linkedin.schema.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NonNull;

@Data
public class FieldPath {
  public static final String EMPTY_FIELD_NAME = " ";
  @NonNull private List<FieldElement> path;
  private boolean isKeySchema;
  private boolean useV2PathsAlways;

  public FieldPath() {
    this.path = new ArrayList<>();
    this.isKeySchema = false;
    this.useV2PathsAlways = true;
  }

  public void setPath(List<FieldElement> path) {
    if (path == null) {
      throw new IllegalArgumentException("Path cannot be null");
    }
    // Ensure that no element in the path is null
    if (path.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("Path cannot contain null elements");
    }
    this.path = path;
  }

  private boolean needsV2Path() {
    if (useV2PathsAlways) {
      return true;
    }
    if (isKeySchema) {
      return true;
    }
    return path.stream()
        .flatMap(element -> element.getType().stream())
        .anyMatch(t -> t.equals("union") || t.equals("array"));
  }

  private void setParentTypeIfNotExists(DataHubType parentType) {
    if (!path.isEmpty() && path.get(path.size() - 1).getParentType() == null) {
      path.get(path.size() - 1).setParentType(parentType);
    }
  }

  private SchemaFieldDataType getTypeOverride() {
    if (!path.isEmpty() && path.get(path.size() - 1).getParentType() != null) {
      return path.get(path.size() - 1).getParentType().asSchemaFieldType();
    }
    return null;
  }

  private String getNativeTypeOverride() {
    SchemaFieldDataType typeOverride = getTypeOverride();
    if (typeOverride != null) {
      if (typeOverride.getType().isArrayType()) {
        ArrayType arrayType = typeOverride.getType().getArrayType();
        return String.format(
            "array(%s)",
            arrayType.getNestedType() != null ? String.join(",", arrayType.getNestedType()) : "");
      } else if (typeOverride.getType().isMapType()) {
        MapType mapType = typeOverride.getType().getMapType();
        return String.format("map(str,%s)", mapType.getValueType());
      }
    }
    return null;
  }

  public String getRecursive(Map<String, Object> schema) {
    String schemaStr = schema.toString();
    for (FieldElement p : path) {
      for (int i = 0; i < p.getSchemaTypes().size(); i++) {
        if (p.getSchemaTypes().get(i).equals(schemaStr)) {
          return p.getType().get(i);
        }
      }
    }
    return null;
  }

  public FieldPath popLast() {
    FieldPath fpath = new FieldPath();
    fpath.setKeySchema(isKeySchema);
    fpath.setPath(new ArrayList<>(path));
    fpath.getPath().remove(fpath.getPath().size() - 1);
    return fpath;
  }

  public FieldPath clonePlus(FieldElement element) {
    FieldPath fpath = new FieldPath();
    fpath.setKeySchema(isKeySchema);
    fpath.setPath(new ArrayList<>(path));
    fpath.getPath().add(element);
    return fpath;
  }

  // TODO: Why is typeSchema an Object?
  public FieldPath expandType(String type, Object typeSchema) {
    FieldPath fpath = new FieldPath();
    fpath.setKeySchema(isKeySchema);
    fpath.setPath(path.stream().map(FieldElement::clone).collect(Collectors.toList()));

    if (!fpath.getPath().isEmpty()) {
      FieldElement lastElement = fpath.getPath().get(fpath.getPath().size() - 1);
      lastElement.getType().add(type);
      lastElement.getSchemaTypes().add(typeSchema.toString());
    } else {
      fpath
          .getPath()
          .add(
              new FieldElement(
                  new ArrayList<>(Collections.singletonList(type)),
                  new ArrayList<>(Collections.singletonList(typeSchema.toString())),
                  null,
                  null));
    }
    return fpath;
  }

  public boolean hasFieldName() {
    return path.stream().anyMatch(f -> f.getName() != null);
  }

  public boolean ensureFieldName() {
    if (!hasFieldName()) {
      if (path.isEmpty()) {
        path.add(new FieldElement(new ArrayList<>(), new ArrayList<>(), null, null));
      }
      path.get(path.size() - 1).setName(EMPTY_FIELD_NAME);
    }
    return true;
  }

  public String asString() {
    boolean v2Format = needsV2Path();
    List<String> prefix = new ArrayList<>();

    if (v2Format) {
      prefix.add("[version=2.0]");
      if (isKeySchema) {
        prefix.add("[key=True]");
      }
    }

    if (!path.isEmpty()) {
      return String.join(".", prefix)
          + "."
          + path.stream().map(f -> f.asString(v2Format)).collect(Collectors.joining("."));
    } else {
      return String.join(".", prefix);
    }
  }

  public String dump() {
    StringBuilder sb = new StringBuilder();
    sb.append("FieldPath: ");
    sb.append(this.asString());
    for (FieldElement f : path) {
      sb.append(f.getName());
      sb.append(" ");
      sb.append(f.getSchemaTypes().toString());
    }
    return sb.toString();
  }
}
