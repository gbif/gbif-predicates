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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
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
public class EsQueryVisitor<S extends SearchParameter> implements QueryVisitor {

  private final EsFieldMapper<S> esFieldMapper;

  private String getExactMatchFieldName(IsNotNullPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getParameter())) {
      return esFieldMapper.getChecklistField(predicate.getChecklistKey(), predicate.getParameter());
    }

    return esFieldMapper.getExactMatchFieldName(predicate.getParameter());
  }

  private String getExactMatchFieldName(IsNullPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getParameter())) {
      return esFieldMapper.getChecklistField(predicate.getChecklistKey(), predicate.getParameter());
    }

    return esFieldMapper.getExactMatchFieldName(predicate.getParameter());
  }

  private String getExactMatchOrVerbatimField(SimplePredicate<S> predicate) {
    if (predicate instanceof EqualsPredicate) {
      EqualsPredicate<S> equalsPredicate = (EqualsPredicate<S>) predicate;
      if (esFieldMapper.isTaxonomic(equalsPredicate.getKey())) {
        return esFieldMapper.getChecklistField(
            equalsPredicate.getChecklistKey(), predicate.getKey());
      }
    }
    if (predicate instanceof LikePredicate) {
      LikePredicate<S> likePredicate = (LikePredicate<S>) predicate;
      if (esFieldMapper.isTaxonomic(likePredicate.getKey())) {
        return esFieldMapper.getChecklistField(likePredicate.getChecklistKey(), predicate.getKey());
      }
    }

    return predicate.isMatchCase()
        ? esFieldMapper.getVerbatimFieldName(predicate.getKey())
        : esFieldMapper.getExactMatchFieldName(predicate.getKey());
  }

  private String getExactMatchOrVerbatimField(InPredicate<S> predicate) {
    if (esFieldMapper.isTaxonomic(predicate.getKey())) {
      return esFieldMapper.getChecklistField(predicate.getChecklistKey(), predicate.getKey());
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
  @Override
  public String buildQuery(Predicate predicate) throws QueryBuildingException {
    return getQueryBuilder(predicate).orElse(QueryBuilders.matchAllQuery()).toString();
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into
   * a json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   * @return body clause
   */
  public Optional<QueryBuilder> getQueryBuilder(Predicate predicate) throws QueryBuildingException {
    if (predicate != null) {
      BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
      visit(predicate, new QueryData(queryBuilder));
      return Optional.of(queryBuilder);
    }
    return Optional.empty();
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(ConjunctionPredicate predicate, QueryData queryData) {
    // must query structure is equivalent to AND
    Map<String, List<QueryBuilder>> queriesByNestedPath = new HashMap<>();
    boolean nonNestedQueriesFound = false;

    for (Predicate subPredicate : predicate.getPredicates()) {
      try {
        BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
        QueryData mustQueryData = new QueryData(mustQueryBuilder);
        visit(subPredicate, mustQueryData);

        if (mustQueryData.isNested()) {
          queriesByNestedPath
              .computeIfAbsent(mustQueryData.nestedPath, k -> new ArrayList<>())
              .add(addNullableFieldPredicate(subPredicate, mustQueryData.rawQueries.get(0)));
        } else {
          queryData.queryBuilder.filter(addNullableFieldPredicate(subPredicate, mustQueryBuilder));
          nonNestedQueriesFound = true;
        }

      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    }

    for (Map.Entry<String, List<QueryBuilder>> entry : queriesByNestedPath.entrySet()) {
      String key = entry.getKey();
      List<QueryBuilder> value = entry.getValue();
      BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();
      value.forEach(q -> nestedBoolQuery.filter().add(q));
      queryData
          .queryBuilder
          .filter()
          .add(QueryBuilders.nestedQuery(key, nestedBoolQuery, ScoreMode.None));
      queryData.nestedPath = !nonNestedQueriesFound ? key : null;
      queryData.rawQueries = nestedBoolQuery.filter();
    }
  }

  /**
   * handle disjunction predicate
   *
   * @param predicate disjunction predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(DisjunctionPredicate predicate, QueryData queryData) {
    Map<S, List<EqualsPredicate<S>>> equalsPredicatesReplaceableByIn = groupEquals(predicate);

    Map<String, List<QueryBuilder>> queriesByNestedPath = new HashMap<>();
    boolean nonNestedQueriesFound = false;

    for (Predicate subPredicate : predicate.getPredicates()) {
      try {
        if (!isReplaceableByInPredicate(subPredicate, equalsPredicatesReplaceableByIn)) {
          BoolQueryBuilder shouldQueryBuilder = QueryBuilders.boolQuery();
          QueryData shouldQueryData = new QueryData(shouldQueryBuilder);
          visit(subPredicate, shouldQueryData);
          QueryBuilder q = addNullableFieldPredicate(subPredicate, shouldQueryBuilder);

          if (shouldQueryData.isNested()) {
            queriesByNestedPath
                .computeIfAbsent(shouldQueryData.nestedPath, k -> new ArrayList<>())
                .addAll(shouldQueryData.rawQueries);
          } else {
            nonNestedQueriesFound = true;
            queryData.queryBuilder.should(q);
          }

          if (subPredicate instanceof SimplePredicate) {
            equalsPredicatesReplaceableByIn.remove(((SimplePredicate) subPredicate).getKey());
          }
        }
      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    }

    for (Map.Entry<String, List<QueryBuilder>> e : queriesByNestedPath.entrySet()) {
      String s = e.getKey();
      List<QueryBuilder> builders = e.getValue();
      BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();
      builders.forEach(q -> nestedBoolQuery.should().add(q));
      queryData
          .queryBuilder
          .should()
          .add(QueryBuilders.nestedQuery(s, nestedBoolQuery, ScoreMode.None));
      queryData.nestedPath = !nonNestedQueriesFound ? s : null;
      queryData.rawQueries = nestedBoolQuery.should();
    }

    if (!equalsPredicatesReplaceableByIn.isEmpty()) {
      if (equalsPredicatesReplaceableByIn.containsKey(OccurrenceSearchParameter.GEOLOGICAL_TIME)) {
        List<QueryBuilder> queryBuilders = new ArrayList<>();
        equalsPredicatesReplaceableByIn
            .get(OccurrenceSearchParameter.GEOLOGICAL_TIME)
            .forEach(
                ep -> {
                  queryBuilders.add(
                      QueryBuilders.termQuery(getExactMatchOrVerbatimField(ep), ep.getValue()));
                });

        if (!queryBuilders.isEmpty()) {
          nonNestedQueriesFound = true;
          queryData.queryBuilder.should().addAll(queryBuilders);
        }

        equalsPredicatesReplaceableByIn.remove(OccurrenceSearchParameter.GEOLOGICAL_TIME);
      }

      if (!equalsPredicatesReplaceableByIn.isEmpty()) {
        Map<String, List<QueryBuilder>> replaceableQueriesByNestedPath = new HashMap<>();
        for (InPredicate<S> ep : toInPredicates(equalsPredicatesReplaceableByIn)) {
          TermsQueryBuilder termsQueryBuilder =
              QueryBuilders.termsQuery(
                  getExactMatchOrVerbatimField(ep),
                  ep.getValues().stream()
                      .map(v -> parseParamValue(v, ep.getKey()))
                      .collect(Collectors.toList()));

          if (esFieldMapper.isNestedField(ep.getKey())) {
            replaceableQueriesByNestedPath
                .computeIfAbsent(esFieldMapper.getNestedPath(ep.getKey()), k -> new ArrayList<>())
                .add(termsQueryBuilder);
          } else {
            queryData.queryBuilder.should().add(termsQueryBuilder);
            nonNestedQueriesFound = true;
          }
        }

        for (Map.Entry<String, List<QueryBuilder>> entry :
            replaceableQueriesByNestedPath.entrySet()) {
          String key = entry.getKey();
          List<QueryBuilder> value = entry.getValue();
          BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();
          value.forEach(q -> nestedBoolQuery.should().add(q));
          queryData
              .queryBuilder
              .should()
              .add(QueryBuilders.nestedQuery(key, nestedBoolQuery, ScoreMode.None));
          queryData.nestedPath = !nonNestedQueriesFound ? key : null;
          queryData.rawQueries = nestedBoolQuery.should();
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

    QueryBuilder q = null;
    if ((Number.class.isAssignableFrom(predicate.getKey().type())
            || predicate.getKey() == OccurrenceSearchParameter.GEOLOGICAL_TIME)
        && SearchTypeValidator.isNumericRange(predicate.getValue())) {
      Range<Double> decimalRange = SearchTypeValidator.parseDecimalRange(predicate.getValue());
      RangeQueryBuilder rqb =
          QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter));
      if (decimalRange.hasLowerBound()) {
        rqb.gte(decimalRange.lowerEndpoint());
      }
      if (decimalRange.hasUpperBound()) {
        rqb.lte(decimalRange.upperEndpoint());
      }

      if (predicate.getKey() == OccurrenceSearchParameter.GEOLOGICAL_TIME) {
        rqb.relation("within");
      }

      q = addNullableFieldPredicate(predicate, rqb);
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())
        && SearchTypeValidator.isDateRange(predicate.getValue())) {
      // The date range is closed-open, so we need lower ≤ date < upper.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      RangeQueryBuilder rqb =
          QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter));
      if (dateRange.hasLowerBound()) {
        rqb.gte(dateRange.lowerEndpoint());
      }
      if (dateRange.hasUpperBound()) {
        rqb.lt(dateRange.upperEndpoint());
      }
      // For a match, the occurrence's date range must be entirely within the search query date
      // range.
      // i.e. Q:eventDate=1980 will match rec:eventDate=1980-02, but not
      // rec:eventDate=1980-10-01/1982-02-02.
      rqb.relation("within");
      q = addNullableFieldPredicate(predicate, rqb);
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      RangeQueryBuilder rqb =
          QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter));
      if (dateRange.hasLowerBound()) {
        rqb.gte(dateRange.lowerEndpoint());
      }
      if (dateRange.hasUpperBound()) {
        rqb.lt(dateRange.upperEndpoint());
      }
      // For a match, the occurrence's date range must be entirely within the search query date
      // range.
      // i.e. Q:eventDate=1980 will match rec:eventDate=1980-02, but not
      // rec:eventDate=1980-10-01/1982-02-02.
      rqb.relation("within");
      q = addNullableFieldPredicate(predicate, rqb);
    } else {
      q =
          QueryBuilders.termQuery(
              getExactMatchOrVerbatimField(predicate),
              parseParamValue(predicate.getValue(), parameter));
    }

    if (esFieldMapper.isNestedField(parameter)) {
      queryData
          .queryBuilder
          .filter()
          .add(
              QueryBuilders.nestedQuery(esFieldMapper.getNestedPath(parameter), q, ScoreMode.None));
      queryData.rawQueries = List.of(q);
      queryData.nestedPath = esFieldMapper.getNestedPath(parameter);
    } else {
      queryData.queryBuilder.filter().add(q);
    }
  }

  private Function<String, Object> parseDate =
      d -> "*".equals(d) ? null : IsoDateParsingUtils.parseDate(d);

  private Function<String, Object> parseInteger = d -> "*".equals(d) ? null : Integer.parseInt(d);

  private Function<String, Object> parseDouble = d -> "*".equals(d) ? null : Double.parseDouble(d);

  public void visit(RangePredicate<S> predicate, QueryData queryData)
      throws QueryBuildingException {

    RangeQueryBuilder rqb =
        QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(predicate.getKey()));

    if (Integer.class.isAssignableFrom(predicate.getKey().type())) {
      initialiseRangeQuery(predicate, rqb, parseInteger);
    } else if (Double.class.isAssignableFrom(predicate.getKey().type())) {
      initialiseRangeQuery(predicate, rqb, parseDouble);
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      initialiseRangeQuery(predicate, rqb, parseDate);
    }

    addFilterQuery(rqb, queryData, predicate.getKey());
  }

  private void initialiseRangeQuery(
      RangePredicate<S> predicate, RangeQueryBuilder rqb, Function<String, Object> initialiser) {
    if (predicate.getValue().getGte() != null) {
      rqb.gte(initialiser.apply(predicate.getValue().getGte()));
    }
    if (predicate.getValue().getLte() != null) {
      rqb.lte(initialiser.apply(predicate.getValue().getLte()));
    }
    if (predicate.getValue().getGt() != null) {
      rqb.gt(initialiser.apply(predicate.getValue().getGt()));
    }
    if (predicate.getValue().getLt() != null) {
      rqb.lt(initialiser.apply(predicate.getValue().getLt()));
    }
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
    QueryBuilder q =
        addNullableFieldPredicate(
            predicate,
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .gte(parseParamValue(predicate.getValue(), parameter)));
    addFilterQuery(q, queryData, predicate.getKey());
  }

  /**
   * Adds an "is null" filter if the mapper instructs to. Used mostly in range queries to give
   * specific semantics to null values.
   */
  private QueryBuilder addNullableFieldPredicate(Predicate predicate, QueryBuilder filter) {
    if (predicate instanceof SimplePredicate
        && esFieldMapper.includeNullInPredicate((SimplePredicate<S>) predicate)) {
      return QueryBuilders.boolQuery()
          .should(filter)
          .should(
              QueryBuilders.boolQuery()
                  .mustNot(
                      QueryBuilders.existsQuery(
                          esFieldMapper.getExactMatchFieldName(
                              ((SimplePredicate<S>) predicate).getKey()))));
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
    QueryBuilder q =
        addNullableFieldPredicate(
            predicate,
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .gt(parseParamValue(predicate.getValue(), parameter)));
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

    // EVENT_DATE needs special handling
    if (parameter == OccurrenceSearchParameter.EVENT_DATE) {
      predicate
          .getValues()
          .forEach(
              value -> {
                try {
                  BoolQueryBuilder shouldQueryBuilder = QueryBuilders.boolQuery();
                  Predicate subPredicate =
                      new EqualsPredicate<>(
                          OccurrenceSearchParameter.EVENT_DATE,
                          value,
                          predicate.isMatchCase(),
                          predicate.getChecklistKey());
                  visit(subPredicate, new QueryData(shouldQueryBuilder));
                  queryData.queryBuilder.should(
                      addNullableFieldPredicate(subPredicate, shouldQueryBuilder));
                } catch (QueryBuildingException ex) {
                  throw new RuntimeException(ex);
                }
              });
    } else {

      TermsQueryBuilder termsQueryBuilder =
          QueryBuilders.termsQuery(
              getExactMatchOrVerbatimField(predicate),
              predicate.getValues().stream()
                  .map(v -> parseParamValue(v, parameter))
                  .collect(Collectors.toList()));

      addFilterQuery(termsQueryBuilder, queryData, predicate.getKey());
    }
  }

  /**
   * handles less than or equals predicate
   *
   * @param predicate less than or equals
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(LessThanOrEqualsPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    QueryBuilder q =
        addNullableFieldPredicate(
            predicate,
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .lte(parseParamValue(predicate.getValue(), parameter)));
    addFilterQuery(q, queryData, parameter);
  }

  /**
   * handles less than predicate
   *
   * @param predicate less than predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(LessThanPredicate<S> predicate, QueryData queryData) {
    S parameter = predicate.getKey();
    QueryBuilder q =
        addNullableFieldPredicate(
            predicate,
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .lt(parseParamValue(predicate.getValue(), parameter)));
    addFilterQuery(q, queryData, parameter);
  }

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(LikePredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        QueryBuilders.wildcardQuery(getExactMatchOrVerbatimField(predicate), predicate.getValue()),
        queryData,
        predicate.getKey());
  }

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(NotPredicate predicate, QueryData queryData) throws QueryBuildingException {
    BoolQueryBuilder mustNotQueryBuilder = QueryBuilders.boolQuery();
    QueryData mustNotQueryData = new QueryData(mustNotQueryBuilder);
    visit(predicate.getPredicate(), mustNotQueryData);
    queryData.queryBuilder.mustNot(mustNotQueryBuilder);
  }

  /**
   * handles within predicate
   *
   * @param within Within predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(WithinPredicate within, QueryData queryData) {
    QueryBuilder q = buildGeoShapeQuery(within.getGeometry());
    queryData.queryBuilder.filter(q);
  }

  public GeoShapeQueryBuilder buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    Function<Polygon, PolygonBuilder> polygonToBuilder =
        polygon -> {
          PolygonBuilder polygonBuilder =
              new PolygonBuilder(
                  new CoordinatesBuilder()
                      .coordinates(
                          normalizePolygonCoordinates(polygon.getExteriorRing().getCoordinates())));
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonBuilder.hole(
                new LineStringBuilder(
                    new CoordinatesBuilder()
                        .coordinates(
                            normalizePolygonCoordinates(
                                polygon.getInteriorRingN(i).getCoordinates()))));
          }
          return polygonBuilder;
        };

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

    ShapeBuilder shapeBuilder = null;
    if (("POINT").equals(type)) {
      shapeBuilder = new PointBuilder(geometry.getCoordinate().x, geometry.getCoordinate().y);
    } else if ("LINESTRING".equals(type)) {
      shapeBuilder = new LineStringBuilder(Arrays.asList(geometry.getCoordinates()));
    } else if ("POLYGON".equals(type)) {
      shapeBuilder = polygonToBuilder.apply((Polygon) geometry);
    } else if ("MULTIPOLYGON".equals(type)) {
      // multipolygon
      MultiPolygonBuilder multiPolygonBuilder = new MultiPolygonBuilder();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        multiPolygonBuilder.polygon(polygonToBuilder.apply((Polygon) geometry.getGeometryN(i)));
      }
      shapeBuilder = multiPolygonBuilder;
    } else {
      throw new IllegalArgumentException(type + " shape is not supported");
    }

    try {
      return QueryBuilders.geoShapeQuery(
              esFieldMapper.getGeoShapeField(), shapeBuilder.buildGeometry())
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
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

  /**
   * handles geoDistance predicate
   *
   * @param geoDistance GeoDistance predicate
   * @param queryData data with the root query builder and the nested path
   */
  public void visit(GeoDistancePredicate geoDistance, QueryData queryData) {
    QueryBuilder q = buildGeoDistanceQuery(geoDistance.getGeoDistance());
    queryData.queryBuilder.filter(q);
  }

  /** Builds a GeoDistance query. */
  private GeoDistanceQueryBuilder buildGeoDistanceQuery(DistanceUnit.GeoDistance geoDistance) {
    return QueryBuilders.geoDistanceQuery(esFieldMapper.getGeoDistanceField())
        .distance(geoDistance.getDistance().toString())
        .point(geoDistance.getLatitude(), geoDistance.getLongitude());
  }

  /**
   * handles ISNOTNULL Predicate
   *
   * @param predicate ISNOTNULL predicate
   */
  public void visit(IsNotNullPredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        QueryBuilders.existsQuery(getExactMatchFieldName(predicate)),
        queryData,
        predicate.getParameter());
  }

  /**
   * handles ISNULL Predicate
   *
   * @param predicate ISNULL predicate
   */
  public void visit(IsNullPredicate<S> predicate, QueryData queryData) {
    addFilterQuery(
        QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.existsQuery(getExactMatchFieldName(predicate))),
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

  private void addFilterQuery(QueryBuilder q, QueryData queryData, S searchParameter) {
    if (esFieldMapper.isNestedField(searchParameter)) {
      queryData
          .queryBuilder
          .filter()
          .add(
              QueryBuilders.nestedQuery(
                  esFieldMapper.getNestedPath(searchParameter), q, ScoreMode.None));
      queryData.rawQueries = List.of(q);
      queryData.nestedPath = esFieldMapper.getNestedPath(searchParameter);
    } else {
      queryData.queryBuilder.filter().add(q);
    }
  }

  @Data
  public static class QueryData {
    BoolQueryBuilder queryBuilder;
    // raw queries that go inside a nested query. For complex predicates they need to be
    // grouped into the same query so it's convenient to have them separated in this field.
    // Otherwise they'd be inside a nested query
    List<QueryBuilder> rawQueries;
    String nestedPath;

    QueryData(BoolQueryBuilder queryBuilder) {
      this.queryBuilder = queryBuilder;
    }

    boolean isNested() {
      return nestedPath != null;
    }
  }
}
