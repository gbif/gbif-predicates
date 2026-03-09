/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.predicate.query;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.GeoShapeRelation;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoShapeFieldQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import jakarta.json.stream.JsonGenerator;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.*;
import org.gbif.api.query.QueryVisitor;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * The class translates predicates in GBIF download API to equivalent Elasticsearch query requests.
 * The json string provided by {@link #buildQuery(Predicate)} can be used with _search get requests
 * of ES index to produce downloads.
 */
@RequiredArgsConstructor
@Slf4j
public abstract class EsQueryVisitor<S extends SearchParameter> implements QueryVisitor {

  private final EsFieldMapper<S> esFieldMapper;
  private final String defaultChecklistKey;

  private String getChecklistKey(Predicate predicate) {

    if (predicate == null) return null;

    Class<?> clazz = predicate.getClass();

    try {
      Method m = clazz.getMethod("getChecklistKey");
      String checklistKey = (String) m.invoke(predicate);
      if (checklistKey != null) return checklistKey;
      return defaultChecklistKey;
    } catch (Exception e) {
      return null;
    }
  }

  private String getExactMatchFieldName(IsNotNullPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getParameter())) {
      return esFieldMapper.getChecklistField(getChecklistKey(predicate), predicate.getParameter());
    }

    return esFieldMapper.getExactMatchFieldName(predicate.getParameter());
  }

  private String getExactMatchFieldName(IsNullPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getParameter())) {
      return esFieldMapper.getChecklistField(getChecklistKey(predicate), predicate.getParameter());
    }

    return esFieldMapper.getExactMatchFieldName(predicate.getParameter());
  }

  private String getExactMatchOrVerbatimField(SimplePredicate<S> predicate) {
    if (predicate instanceof EqualsPredicate) {
      EqualsPredicate<S> equalsPredicate = (EqualsPredicate<S>) predicate;
      if (esFieldMapper.isTaxonomic(equalsPredicate.getKey())) {
        return esFieldMapper.getChecklistField(
            getChecklistKey(equalsPredicate), predicate.getKey());
      }
    }
    if (predicate instanceof LikePredicate) {
      LikePredicate<S> likePredicate = (LikePredicate<S>) predicate;
      if (esFieldMapper.isTaxonomic(likePredicate.getKey())) {
        return esFieldMapper.getChecklistField(getChecklistKey(likePredicate), predicate.getKey());
      }
    }

    return predicate.isMatchCase()
        ? esFieldMapper.getVerbatimFieldName(predicate.getKey())
        : esFieldMapper.getExactMatchFieldName(predicate.getKey());
  }

  private String getExactMatchOrVerbatimField(InPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getKey())) {
      return esFieldMapper.getChecklistField(getChecklistKey(predicate), predicate.getKey());
    }

    return Optional.ofNullable(predicate.isMatchCase()).orElse(Boolean.FALSE)
        ? esFieldMapper.getVerbatimFieldName(predicate.getKey())
        : esFieldMapper.getExactMatchFieldName(predicate.getKey());
  }

  private String parseParamValue(String value, S parameter) {
    if (Enum.class.isAssignableFrom(parameter.type())
        && !Country.class.isAssignableFrom(parameter.type())) {
      return VocabularyUtils.lookup(value, (Class<Enum<?>>) parameter.type())
          .map(Enum::name)
          .orElse(null);
    }
    if (Boolean.class.isAssignableFrom(parameter.type())) {
      return value.toLowerCase();
    }
    return value;
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into
   * a json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   * @return body clause
   */
  private static final JsonpMapper JSONP_MAPPER = new JacksonJsonpMapper();

  @Override
  public String buildQuery(Predicate predicate) throws QueryBuildingException {
    return getQueryBuilder(predicate)
        .map(EsQueryVisitor::queryToJson)
        .orElseGet(EsQueryVisitor::matchAllQueryJson);
  }

  private static String queryToJson(Query query) {
    StringWriter sw = new StringWriter();
    try (JsonGenerator g = JSONP_MAPPER.jsonProvider().createGenerator(sw)) {
      query.serialize(g, JSONP_MAPPER);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return sw.toString();
  }

  private static String matchAllQueryJson() {
    return queryToJson(Query.of(q -> q.matchAll(m -> m)));
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into
   * a json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   * @return body clause
   */
  public Optional<Query> getQueryBuilder(Predicate predicate) throws QueryBuildingException {
    if (predicate != null) {
      QueryData queryData = new QueryData();
      visit(predicate, queryData);
      return Optional.of(buildRootBoolQuery(queryData));
    }
    return Optional.empty();
  }

  private static Query buildRootBoolQuery(QueryData queryData) {
    return buildBoolQuery(
        queryData.filterQueries, queryData.shouldQueries, queryData.mustNotQueries);
  }

  private static Query buildBoolQuery(List<Query> filter, List<Query> should, List<Query> mustNot) {
    return Query.of(
        q ->
            q.bool(
                b -> {
                  if (filter != null && !filter.isEmpty()) b.filter(filter);
                  if (should != null && !should.isEmpty()) b.should(should);
                  if (mustNot != null && !mustNot.isEmpty()) b.mustNot(mustNot);
                  return b;
                }));
  }

  private static Query termQuery(String field, Object value) {
    return Query.of(q -> q.term(t -> t.field(field).value(FieldValue.of(value))));
  }

  private static Query termsQuery(String field, List<Object> values) {
    return Query.of(
        q ->
            q.terms(
                t ->
                    t.field(field)
                        .terms(
                            v ->
                                v.value(
                                    values.stream()
                                        .map(FieldValue::of)
                                        .collect(Collectors.toList())))));
  }

  private static Query existsQuery(String field) {
    return Query.of(q -> q.exists(e -> e.field(field)));
  }

  private static Query wildcardQuery(String field, String value) {
    return Query.of(q -> q.wildcard(w -> w.field(field).value(value)));
  }

  private static Query nestedQuery(String path, Query inner) {
    return Query.of(q -> q.nested(n -> n.path(path).query(inner).scoreMode(ChildScoreMode.None)));
  }

  /**
   * Relation for range fields: "within", "contains", or "intersects". Null = default (intersects).
   */
  /** Ensure value serializes to JSON (e.g. LocalDate -> string) to avoid Jackson errors. */
  private static Object toJsonSafe(Object value) {
    if (value == null) return null;
    if (value instanceof LocalDate) return value.toString();
    return value;
  }

  private static Query rangeQuery(
      String field, Object gte, Object lte, Object gt, Object lt, String relation) {
    return Query.of(
        q ->
            q.range(
                r ->
                    r.untyped(
                        u -> {
                          u.field(field);
                          if (gte != null) u.gte(JsonData.of(toJsonSafe(gte)));
                          if (lte != null) u.lte(JsonData.of(toJsonSafe(lte)));
                          if (gt != null) u.gt(JsonData.of(toJsonSafe(gt)));
                          if (lt != null) u.lt(JsonData.of(toJsonSafe(lt)));
                          return u;
                        })));
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(ConjunctionPredicate predicate, QueryData queryData) {
    Map<String, List<Query>> queriesByNestedPath = new HashMap<>();
    boolean nonNestedQueriesFound = false;

    for (Predicate subPredicate : predicate.getPredicates()) {
      try {
        QueryData mustQueryData = new QueryData();
        visit(subPredicate, mustQueryData);

        if (mustQueryData.isNested()) {
          Query inner = addNullableFieldPredicate(subPredicate, mustQueryData.rawQueries.get(0));
          queriesByNestedPath
              .computeIfAbsent(mustQueryData.nestedPath, k -> new ArrayList<>())
              .add(inner);
        } else {
          Query combined = buildBoolFromQueryData(mustQueryData);
          queryData.filterQueries.add(addNullableFieldPredicate(subPredicate, combined));
          nonNestedQueriesFound = true;
        }
      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    }

    for (Map.Entry<String, List<Query>> entry : queriesByNestedPath.entrySet()) {
      String key = entry.getKey();
      List<Query> value = entry.getValue();
      Query nestedBool = buildBoolQuery(value, Collections.emptyList(), Collections.emptyList());
      queryData.filterQueries.add(nestedQuery(key, nestedBool));
      queryData.nestedPath = !nonNestedQueriesFound ? key : null;
      queryData.rawQueries = value;
    }
  }

  private static Query buildBoolFromQueryData(QueryData d) {
    return buildBoolQuery(d.filterQueries, d.shouldQueries, d.mustNotQueries);
  }

  /**
   * handle disjunction predicate
   *
   * @param predicate disjunction predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(DisjunctionPredicate predicate, QueryData queryData) {
    Map<S, List<EqualsPredicate<S>>> equalsPredicatesReplaceableByIn = groupEquals(predicate);

    Map<String, List<Query>> queriesByNestedPath = new HashMap<>();
    boolean nonNestedQueriesFound = false;

    for (Predicate subPredicate : predicate.getPredicates()) {
      try {
        if (!isReplaceableByInPredicate(subPredicate, equalsPredicatesReplaceableByIn)) {
          QueryData shouldQueryData = new QueryData();
          visit(subPredicate, shouldQueryData);
          Query q =
              addNullableFieldPredicate(subPredicate, buildBoolFromQueryData(shouldQueryData));

          if (shouldQueryData.isNested()) {
            queriesByNestedPath
                .computeIfAbsent(shouldQueryData.nestedPath, k -> new ArrayList<>())
                .addAll(shouldQueryData.rawQueries);
          } else {
            nonNestedQueriesFound = true;
            queryData.shouldQueries.add(q);
          }

          if (subPredicate instanceof SimplePredicate) {
            equalsPredicatesReplaceableByIn.remove(((SimplePredicate) subPredicate).getKey());
          }
        }
      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    }

    for (Map.Entry<String, List<Query>> e : queriesByNestedPath.entrySet()) {
      String s = e.getKey();
      List<Query> builders = e.getValue();
      Query nestedBool = buildBoolQuery(Collections.emptyList(), builders, Collections.emptyList());
      queryData.shouldQueries.add(nestedQuery(s, nestedBool));
      queryData.nestedPath = !nonNestedQueriesFound ? s : null;
      queryData.rawQueries = builders;
    }

    if (!equalsPredicatesReplaceableByIn.isEmpty()) {
      Optional<S> geoTimeParam = getParam(OccurrenceSearchParameter.GEOLOGICAL_TIME.name());
      if (geoTimeParam.isPresent()
          && equalsPredicatesReplaceableByIn.containsKey(geoTimeParam.get())) {
        List<Query> queryBuilders = new ArrayList<>();
        equalsPredicatesReplaceableByIn
            .get(geoTimeParam.get())
            .forEach(
                ep ->
                    queryBuilders.add(
                        termQuery(
                            getExactMatchOrVerbatimField(ep),
                            parseParamValue(ep.getValue(), ep.getKey()))));

        if (!queryBuilders.isEmpty()) {
          nonNestedQueriesFound = true;
          queryData.shouldQueries.addAll(queryBuilders);
        }
        equalsPredicatesReplaceableByIn.remove(geoTimeParam.get());
      }

      if (!equalsPredicatesReplaceableByIn.isEmpty()) {
        Map<String, List<Query>> replaceableQueriesByNestedPath = new HashMap<>();
        for (InPredicate<S> ep : toInPredicates(equalsPredicatesReplaceableByIn)) {
          Query termsQ =
              termsQuery(
                  getExactMatchOrVerbatimField(ep),
                  ep.getValues().stream()
                      .map(v -> parseParamValue(v, ep.getKey()))
                      .collect(Collectors.toList()));

          if (esFieldMapper.isNestedField(ep.getKey())) {
            replaceableQueriesByNestedPath
                .computeIfAbsent(esFieldMapper.getNestedPath(ep.getKey()), k -> new ArrayList<>())
                .add(termsQ);
          } else {
            queryData.shouldQueries.add(termsQ);
            nonNestedQueriesFound = true;
          }
        }

        for (Map.Entry<String, List<Query>> entry : replaceableQueriesByNestedPath.entrySet()) {
          String key = entry.getKey();
          List<Query> value = entry.getValue();
          Query nestedBool =
              buildBoolQuery(Collections.emptyList(), value, Collections.emptyList());
          queryData.shouldQueries.add(nestedQuery(key, nestedBool));
          queryData.nestedPath = !nonNestedQueriesFound ? key : null;
          queryData.rawQueries = value;
        }
      }
    }
  }

  /** Checks if a predicate has in grouped and can be replaced later by a InPredicate. */
  private boolean isReplaceableByInPredicate(
      Predicate predicate, Map<S, List<EqualsPredicate<S>>> equalsPredicatesReplaceableByIn) {
    if (!equalsPredicatesReplaceableByIn.isEmpty() && predicate instanceof EqualsPredicate) {
      EqualsPredicate<S> equalsPredicate = (EqualsPredicate<S>) predicate;

      // check it is not a range predicate e.g. YEAR 2000,*
      if (SearchTypeValidator.isNumericRange(equalsPredicate.getValue())
          || SearchTypeValidator.isDateRange(equalsPredicate.getValue())) {
        return false;
      }

      return equalsPredicatesReplaceableByIn.containsKey(equalsPredicate.getKey())
          && equalsPredicatesReplaceableByIn
              .get(equalsPredicate.getKey())
              .contains(equalsPredicate);
    }
    return false;
  }

  /** Groups all equals predicates by search parameter. */
  private Map<S, List<EqualsPredicate<S>>> groupEquals(DisjunctionPredicate predicate) {
    return predicate.getPredicates().stream()
        .filter(p -> p instanceof EqualsPredicate)
        .filter(p -> ((EqualsPredicate<?>) p).getChecklistKey() == null)
        .map(p -> (EqualsPredicate<S>) p)
        .collect(Collectors.groupingBy(EqualsPredicate::getKey))
        .entrySet()
        .stream()
        .filter(e -> e.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Transforms the grouped EqualsPredicates into InPredicates. */
  private List<InPredicate<S>> toInPredicates(Map<S, List<EqualsPredicate<S>>> equalPredicates) {
    return equalPredicates.entrySet().stream()
        .map(
            e ->
                e.getValue().stream()
                    .collect(Collectors.groupingBy(EqualsPredicate::isMatchCase))
                    .entrySet()
                    .stream()
                    .map(
                        group ->
                            new InPredicate<S>(
                                e.getKey(),
                                group.getValue().stream()
                                    .map(EqualsPredicate::getValue)
                                    .collect(Collectors.toSet()),
                                group.getKey()))
                    .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  /**
   * handles EqualPredicate
   *
   * @param predicate equalPredicate
   */
  public void visit(EqualsPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    String field = esFieldMapper.getExactMatchFieldName(parameter);
    Query q;

    if ((Number.class.isAssignableFrom(predicate.getKey().type())
            || paramEquals(predicate.getKey(), OccurrenceSearchParameter.GEOLOGICAL_TIME.name()))
        && SearchTypeValidator.isNumericRange(predicate.getValue())) {
      Range<Double> decimalRange = SearchTypeValidator.parseDecimalRange(predicate.getValue());
      Double gte = decimalRange.hasLowerBound() ? decimalRange.lowerEndpoint() : null;
      Double lte = decimalRange.hasUpperBound() ? decimalRange.upperEndpoint() : null;
      String rel =
          paramEquals(predicate.getKey(), OccurrenceSearchParameter.GEOLOGICAL_TIME.name())
              ? "within"
              : null;
      q = addNullableFieldPredicate(predicate, rangeQuery(field, gte, lte, null, null, rel));
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())
        && SearchTypeValidator.isDateRange(predicate.getValue())) {
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      Object gte = dateRange.hasLowerBound() ? dateRange.lowerEndpoint().toString() : null;
      Object lt = dateRange.hasUpperBound() ? dateRange.upperEndpoint().toString() : null;
      q = addNullableFieldPredicate(predicate, rangeQuery(field, gte, null, null, lt, "within"));
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      Object gte = dateRange.hasLowerBound() ? dateRange.lowerEndpoint().toString() : null;
      Object lt = dateRange.hasUpperBound() ? dateRange.upperEndpoint().toString() : null;
      q = addNullableFieldPredicate(predicate, rangeQuery(field, gte, null, null, lt, "within"));
    } else {
      q =
          addNullableFieldPredicate(
              predicate,
              termQuery(
                  getExactMatchOrVerbatimField(predicate),
                  parseParamValue(predicate.getValue(), parameter)));
    }

    addFilterQuery(q, queryData, parameter);
  }

  private final Function<String, Object> parseDate =
      d -> "*".equals(d) ? null : IsoDateParsingUtils.parseDate(d);

  private final Function<String, Object> parseInteger =
      d -> "*".equals(d) ? null : Integer.parseInt(d);

  private final Function<String, Object> parseDouble =
      d -> "*".equals(d) ? null : Double.parseDouble(d);

  public void visit(RangePredicate<S> predicate, QueryData queryData)
      throws QueryBuildingException {
    String field = esFieldMapper.getExactMatchFieldName(predicate.getKey());
    Object gte = null, lte = null, gt = null, lt = null;
    Function<String, Object> initialiser;
    if (Integer.class.isAssignableFrom(predicate.getKey().type())) {
      initialiser = parseInteger;
    } else if (Double.class.isAssignableFrom(predicate.getKey().type())) {
      initialiser = parseDouble;
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      initialiser = parseDate;
    } else {
      addFilterQuery(Query.of(q -> q.matchAll(m -> m)), queryData, predicate.getKey());
      return;
    }
    if (predicate.getValue().getGte() != null)
      gte = initialiser.apply(predicate.getValue().getGte());
    if (predicate.getValue().getLte() != null)
      lte = initialiser.apply(predicate.getValue().getLte());
    if (predicate.getValue().getGt() != null) gt = initialiser.apply(predicate.getValue().getGt());
    if (predicate.getValue().getLt() != null) lt = initialiser.apply(predicate.getValue().getLt());
    Query rqb = rangeQuery(field, gte, lte, gt, lt, null);
    addFilterQuery(rqb, queryData, predicate.getKey());
  }

  private static LocalDate parseDate(String d) {
    return "*".equals(d) ? null : IsoDateParsingUtils.parseDate(d);
  }

  /**
   * handle greater than equals predicate
   *
   * @param predicate gte predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(GreaterThanOrEqualsPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    Object val = parseParamValue(predicate.getValue(), parameter);
    Query q =
        addNullableFieldPredicate(
            predicate,
            rangeQuery(
                esFieldMapper.getExactMatchFieldName(parameter), val, null, null, null, null));
    addFilterQuery(q, queryData, predicate.getKey());
  }

  /**
   * Adds an "is null" filter if the mapper instructs to. Used mostly in range queries to give
   * specific semantics to null values.
   */
  private Query addNullableFieldPredicate(Predicate predicate, Query filter) {
    if (predicate instanceof SimplePredicate
        && esFieldMapper.includeNullInPredicate((SimplePredicate<S>) predicate)) {
      String field =
          esFieldMapper.getExactMatchFieldName(((SimplePredicate<S>) predicate).getKey());
      Query missing = Query.of(q -> q.bool(b -> b.mustNot(existsQuery(field))));
      return buildBoolQuery(
          Collections.emptyList(), List.of(filter, missing), Collections.emptyList());
    }
    return filter;
  }

  /**
   * handles greater than predicate
   *
   * @param predicate greater than predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(GreaterThanPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    Object val = parseParamValue(predicate.getValue(), parameter);
    Query q =
        addNullableFieldPredicate(
            predicate,
            rangeQuery(
                esFieldMapper.getExactMatchFieldName(parameter), null, null, val, null, null));
    addFilterQuery(q, queryData, parameter);
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(InPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();

    if (paramEquals(parameter, OccurrenceSearchParameter.EVENT_DATE.name())) {
      predicate
          .getValues()
          .forEach(
              value -> {
                try {
                  QueryData shouldQueryData = new QueryData();
                  Predicate subPredicate =
                      new EqualsPredicate<>(
                          getParam(OccurrenceSearchParameter.EVENT_DATE.name()).get(),
                          value,
                          predicate.isMatchCase(),
                          getChecklistKey(predicate));
                  visit(subPredicate, shouldQueryData);
                  queryData.shouldQueries.add(
                      addNullableFieldPredicate(
                          subPredicate, buildBoolFromQueryData(shouldQueryData)));
                } catch (QueryBuildingException ex) {
                  throw new RuntimeException(ex);
                }
              });
    } else {
      Query termsQ =
          termsQuery(
              getExactMatchOrVerbatimField(predicate),
              predicate.getValues().stream()
                  .map(v -> parseParamValue(v, parameter))
                  .collect(Collectors.toList()));
      addFilterQuery(termsQ, queryData, predicate.getKey());
    }
  }

  /** handles less than or equals predicate */
  public void visit(LessThanOrEqualsPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    Object val = parseParamValue(predicate.getValue(), parameter);
    Query q =
        addNullableFieldPredicate(
            predicate,
            rangeQuery(
                esFieldMapper.getExactMatchFieldName(parameter), null, val, null, null, null));
    addFilterQuery(q, queryData, parameter);
  }

  /** handles less than predicate */
  public void visit(LessThanPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    Object val = parseParamValue(predicate.getValue(), parameter);
    Query q =
        addNullableFieldPredicate(
            predicate,
            rangeQuery(
                esFieldMapper.getExactMatchFieldName(parameter), null, null, null, val, null));
    addFilterQuery(q, queryData, parameter);
  }

  /** handles like predicate */
  public void visit(LikePredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        wildcardQuery(getExactMatchOrVerbatimField(predicate), predicate.getValue()),
        queryData,
        predicate.getKey());
  }

  /** handles not predicate */
  public void visit(NotPredicate predicate, QueryData queryData) throws QueryBuildingException {
    QueryData mustNotQueryData = new QueryData();
    visit(predicate.getPredicate(), mustNotQueryData);
    queryData.mustNotQueries.add(buildBoolFromQueryData(mustNotQueryData));
  }

  /**
   * handles within predicate
   *
   * @param within Within predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(WithinPredicate within, QueryData queryData) {
    Query q = buildGeoShapeQuery(within.getGeometry());
    queryData.filterQueries.add(q);
  }

  /** Builds a geo_shape query (WITHIN relation) from WKT geometry. */
  public Query buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

    Object geoJson;
    if (("POINT").equals(type)) {
      Coordinate c = geometry.getCoordinate();
      geoJson = Map.of("type", "Point", "coordinates", List.of(c.x, c.y));
    } else if ("LINESTRING".equals(type)) {
      geoJson =
          Map.of(
              "type",
              "LineString",
              "coordinates",
              Arrays.stream(geometry.getCoordinates())
                  .map(c -> List.of(c.x, c.y))
                  .collect(Collectors.toList()));
    } else if ("POLYGON".equals(type)) {
      Polygon poly = (Polygon) geometry;
      List<List<List<Double>>> rings = new ArrayList<>();
      rings.add(
          Arrays.stream(normalizePolygonCoordinates(poly.getExteriorRing().getCoordinates()))
              .map(c -> List.<Double>of(c.x, c.y))
              .collect(Collectors.toList()));
      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        rings.add(
            Arrays.stream(normalizePolygonCoordinates(poly.getInteriorRingN(i).getCoordinates()))
                .map(c -> List.<Double>of(c.x, c.y))
                .collect(Collectors.toList()));
      }
      geoJson = Map.of("type", "Polygon", "coordinates", rings);
    } else if ("MULTIPOLYGON".equals(type)) {
      List<List<List<List<Double>>>> polygons = new ArrayList<>();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        Polygon poly = (Polygon) geometry.getGeometryN(i);
        List<List<List<Double>>> rings = new ArrayList<>();
        rings.add(
            Arrays.stream(normalizePolygonCoordinates(poly.getExteriorRing().getCoordinates()))
                .map(c -> List.<Double>of(c.x, c.y))
                .collect(Collectors.toList()));
        for (int h = 0; h < poly.getNumInteriorRing(); h++) {
          rings.add(
              Arrays.stream(normalizePolygonCoordinates(poly.getInteriorRingN(h).getCoordinates()))
                  .map(c -> List.<Double>of(c.x, c.y))
                  .collect(Collectors.toList()));
        }
        polygons.add(rings);
      }
      geoJson = Map.of("type", "MultiPolygon", "coordinates", polygons);
    } else {
      throw new IllegalArgumentException(type + " shape is not supported");
    }

    GeoShapeFieldQuery shapeQuery =
        GeoShapeFieldQuery.of(s -> s.shape(JsonData.of(geoJson)).relation(GeoShapeRelation.Within));
    return Query.of(
        q -> q.geoShape(g -> g.field(esFieldMapper.getGeoShapeField()).shape(shapeQuery)));
  }

  /** Eliminates consecutive duplicates. The order is preserved. */
  static Coordinate[] normalizePolygonCoordinates(Coordinate[] coordinates) {
    List<Coordinate> normalizedCoordinates = new ArrayList<>();

    // we always have to keep the fist and last coordinates
    int i = 0;
    normalizedCoordinates.add(i++, coordinates[0]);

    for (int j = 1; j < coordinates.length; j++) {
      if (!coordinates[j - 1].equals(coordinates[j])) {
        normalizedCoordinates.add(i++, coordinates[j]);
      }
    }

    return normalizedCoordinates.toArray(new Coordinate[0]);
  }

  /** handles geoDistance predicate */
  public void visit(GeoDistancePredicate geoDistance, QueryData queryData) {
    Query q = buildGeoDistanceQuery(geoDistance.getGeoDistance());
    queryData.filterQueries.add(q);
  }

  private Query buildGeoDistanceQuery(DistanceUnit.GeoDistance geoDistance) {
    return Query.of(
        q ->
            q.geoDistance(
                g ->
                    g.field(esFieldMapper.getGeoDistanceField())
                        .distance(geoDistance.getDistance().toString())
                        .location(
                            loc ->
                                loc.latlon(
                                    ll ->
                                        ll.lat(geoDistance.getLatitude())
                                            .lon(geoDistance.getLongitude())))));
  }

  /** handles ISNOTNULL Predicate */
  public void visit(IsNotNullPredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        existsQuery(getExactMatchFieldName(predicate)), queryData, predicate.getParameter());
  }

  /** handles ISNULL Predicate */
  public void visit(IsNullPredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        Query.of(q -> q.bool(b -> b.mustNot(existsQuery(getExactMatchFieldName(predicate))))),
        queryData,
        predicate.getParameter());
  }

  private void visit(Object object, QueryData queryData) throws QueryBuildingException {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass(), QueryData.class);
    } catch (NoSuchMethodException e) {
      log.warn(
          "Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object, queryData);
    } catch (IllegalAccessException e) {
      log.error(
          "This error shouldn't occur if all visit methods are public. Probably a programming error",
          e);
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      log.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }

  private void addFilterQuery(Query q, QueryData queryData, S searchParameter) {
    if (esFieldMapper.isNestedField(searchParameter)) {
      queryData.filterQueries.add(nestedQuery(esFieldMapper.getNestedPath(searchParameter), q));
      queryData.rawQueries = List.of(q);
      queryData.nestedPath = esFieldMapper.getNestedPath(searchParameter);
    } else {
      queryData.filterQueries.add(q);
    }
  }

  @Data
  public static class QueryData {
    List<Query> filterQueries = new ArrayList<>();
    List<Query> shouldQueries = new ArrayList<>();
    List<Query> mustNotQueries = new ArrayList<>();

    /** Raw queries that go inside a nested query. */
    List<Query> rawQueries = new ArrayList<>();

    String nestedPath;

    QueryData() {}

    boolean isNested() {
      return nestedPath != null;
    }
  }

  private boolean paramEquals(S param1, String param2) {
    Optional<S> param2Opt = getParam(param2);
    return param2Opt.isPresent() && param1 == param2Opt.get();
  }

  protected abstract Optional<S> getParam(String name);
}
