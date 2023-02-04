package com.linkedin.data.schema.annotation;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

@Slf4j
public class SearchableAnnotationHandlerImpl extends PegasusSchemaAnnotationHandlerImpl {

    public SearchableAnnotationHandlerImpl(String annotationNameSpace) {
        super(annotationNameSpace);
    }

    @Override
    public SchemaVisitor getVisitor() {
        return new SearchableSchemaVisitor(this);
    }

    public static class SearchableSchemaVisitor extends PathSpecBasedSchemaAnnotationVisitor {
        /**
         * TagAssociation has a reference to `TagUrn` which contains the expected annotation
         * using namespace `com.linkedin.common.TagUrn` however if the version with the extra
         * "urn" (`com.linkedin.common.urn.TagUrn`) in the package name is validated it doesn't
         * include the expected annotation and will fail.
         */
        final private static Set<String> EXCLUDE = Set.of("com.linkedin.common.urn.TagUrn");

        public SearchableSchemaVisitor(SchemaAnnotationHandler handler) {
            super(handler);
        }

        @Override
        public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
            try {
                if (skipValidation(context)) {
                    log.warn("Skipping annotation validation: " + context.getCurrentSchema());
                    return;
                }
                super.callbackOnContext(context, order);
            } catch (AssertionError e) {
                log.error(String.format("CurrentSchema: %s", context.getCurrentSchema()), e);
                throw e;
            }
        }

        private static boolean skipValidation(TraverserContext context) {
            boolean skip = false;

            Map<String, Object> props = Map.of();
            if (context.getCurrentSchema().getType() == DataSchema.Type.TYPEREF) {
                props = context.getCurrentSchema().getProperties();
            } else if (context.getParentSchema() != null &&  context.getParentSchema().getType() == DataSchema.Type.TYPEREF) {
                props = context.getParentSchema().getProperties();
            }

            if (props.containsKey("java")) {
                String pegasusClassName = ((DataMap) props.get("java")).get("class").toString();
                if (EXCLUDE.contains(pegasusClassName)) {
                    skip = true;
                }
            }

            return skip;
        }

    }
}
