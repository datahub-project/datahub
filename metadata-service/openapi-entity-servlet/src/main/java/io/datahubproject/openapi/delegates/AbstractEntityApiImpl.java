package io.datahubproject.openapi.delegates;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.entities.EntitiesController;
import io.datahubproject.openapi.util.OpenApiEntitiesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datahubproject.openapi.util.ReflectionCache.toLowerFirst;

abstract public class AbstractEntityApiImpl<I, O> {
    @Autowired
    protected EntityRegistry _entityRegistry;
    @Autowired
    protected EntityService _entityService;
    @Autowired
    protected EntitiesController _v1Controller;

    protected StackWalker walker = StackWalker.getInstance();

    abstract String getEntityType();

    public ResponseEntity<O> getEntity(String urn, Boolean systemMetadata, List<String> aspects, Class<O> respClazz) {
        String[] requestedAspects = Optional.ofNullable(aspects).map(asp -> asp.stream().distinct().toArray(String[]::new)).orElse(null);
        ResponseEntity<UrnResponseMap> result = _v1Controller.getEntities(new String[]{urn}, requestedAspects);
        return ResponseEntity.of(OpenApiEntitiesUtil.convertEntity(result.getBody(), respClazz, systemMetadata));
    }

    public <A> ResponseEntity<A> getAspect(String urn, Boolean systemMetadata, String aspect, Class<O> entityRespClass,
                                           Class<A> aspectRespClazz) {
        String[] requestedAspects = new String[]{aspect};
        ResponseEntity<UrnResponseMap> result = _v1Controller.getEntities(new String[]{urn}, requestedAspects);
        return ResponseEntity.of(OpenApiEntitiesUtil.convertAspect(result.getBody(), aspect, entityRespClass, aspectRespClazz,
                systemMetadata));
    }

    public ResponseEntity<List<O>> createEntity(List<I> body, Class<I> reqClazz, Class<O> respClazz) {
        List<UpsertAspectRequest> aspects = body.stream()
                .flatMap(b -> OpenApiEntitiesUtil.convertEntityToUpsert(b, reqClazz, _entityRegistry).stream())
                .collect(Collectors.toList());
        _v1Controller.postEntities(aspects);
        List<O> responses = body.stream()
                .map(req -> OpenApiEntitiesUtil.convertToResponse(req, respClazz, _entityRegistry))
                .collect(Collectors.toList());
        return ResponseEntity.ok(responses);
    }

    public <AQ, AR> ResponseEntity<AR> createAspect(String urn, String aspectName, AQ body, Class<AQ> reqClazz, Class<AR> respClazz) {
        UpsertAspectRequest aspectUpsert = OpenApiEntitiesUtil.convertAspectToUpsert(getEntityType(), urn, body, reqClazz);
        _v1Controller.postEntities(Stream.of(aspectUpsert).filter(Objects::nonNull).collect(Collectors.toList()));
        AR response = OpenApiEntitiesUtil.convertToResponseAspect(body, respClazz);
        return ResponseEntity.ok(response);
    }

    public ResponseEntity<Void> deleteEntity(String urn) {
        _v1Controller.deleteEntities(new String[]{urn}, false);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    public ResponseEntity<Void> headEntity(String urn) {
        try {
            Urn entityUrn = Urn.createFromString(urn);
            if (_entityService.exists(entityUrn)) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseEntity<Void> headAspect(String urn, String aspect) {
        try {
            Urn entityUrn = Urn.createFromString(urn);
            if (_entityService.exists(entityUrn, aspect)) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseEntity<Void> deleteAspect(String urn, String aspect) {
        _entityService.deleteAspect(urn, aspect, Map.of(), false);
        _v1Controller.deleteEntities(new String[]{urn}, false);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    protected static String methodNameToAspectName(String methodName) {
        return toLowerFirst(methodName.replaceFirst("^(get|head|delete|create)", ""));
    }
}
