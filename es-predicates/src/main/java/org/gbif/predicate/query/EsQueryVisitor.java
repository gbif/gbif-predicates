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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.predicate.*;
import org.gbif.api.query.QueryBuildingException;
import org.gbif.api.query.QueryVisitor;
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

  private String getExactMatchOrVerbatimField(SimplePredicate<S> predicate) {
    return predicate.isMatchCase()
        ? esFieldMapper.getVerbatimFieldName(predicate.getKey())
        : esFieldMapper.getExactMatchFieldName(predicate.getKey());
  }

  private String getExactMatchOrVerbatimField(InPredicate<S> predicate) {
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
      visit(predicate, queryBuilder);
      return Optional.of(queryBuilder);
    }
    return Optional.empty();
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   * @param queryBuilder root query builder
   */
  public void visit(ConjunctionPredicate predicate, BoolQueryBuilder queryBuilder)
      throws QueryBuildingException {
    // must query structure is equivalent to AND
    predicate
        .getPredicates()
        .forEach(
            subPredicate -> {
              try {
                BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
                visit(subPredicate, mustQueryBuilder);
                queryBuilder.filter(mustQueryBuilder);
              } catch (QueryBuildingException ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  /**
   * handle disjunction predicate
   *
   * @param predicate disjunction predicate
   * @param queryBuilder root query builder
   */
  public void visit(DisjunctionPredicate predicate, BoolQueryBuilder queryBuilder)
      throws QueryBuildingException {
    Map<S, List<EqualsPredicate<S>>> equalsPredicatesReplaceableByIn = groupEquals(predicate);

    predicate
        .getPredicates()
        .forEach(
            subPredicate -> {
              try {
                if (!isReplaceableByInPredicate(subPredicate, equalsPredicatesReplaceableByIn)) {
                  BoolQueryBuilder shouldQueryBuilder = QueryBuilders.boolQuery();
                  visit(subPredicate, shouldQueryBuilder);
                  queryBuilder.should(shouldQueryBuilder);
                }
              } catch (QueryBuildingException ex) {
                throw new RuntimeException(ex);
              }
            });
    if (!equalsPredicatesReplaceableByIn.isEmpty()) {
      toInPredicates(equalsPredicatesReplaceableByIn)
          .forEach(
              ep ->
                  queryBuilder
                      .should()
                      .add(
                          QueryBuilders.termsQuery(
                              getExactMatchOrVerbatimField(ep),
                              ep.getValues().stream()
                                  .map(v -> parseParamValue(v, ep.getKey()))
                                  .collect(Collectors.toList()))));
    }
  }

  /** Checks if a predicate has in grouped and can be replaced later by a InPredicate. */
  private boolean isReplaceableByInPredicate(
      Predicate predicate, Map<S, List<EqualsPredicate<S>>> equalsPredicatesReplaceableByIn) {
    if (!equalsPredicatesReplaceableByIn.isEmpty() && predicate instanceof EqualsPredicate) {
      EqualsPredicate<S> equalsPredicate = (EqualsPredicate<S>) predicate;
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
  public void visit(EqualsPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();

    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      if (SearchTypeValidator.isRange(predicate.getValue())) {
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
        queryBuilder.filter().add(rqb);
        return;
      }
    } else if (Number.class.isAssignableFrom(predicate.getKey().type())) {
      if (SearchTypeValidator.isRange(predicate.getValue())) {
        Range<Double> decimalRange = SearchTypeValidator.parseDecimalRange(predicate.getValue());
        RangeQueryBuilder rqb =
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter));
        if (decimalRange.hasLowerBound()) {
          rqb.gte(decimalRange.lowerEndpoint());
        }
        if (decimalRange.hasUpperBound()) {
          rqb.lte(decimalRange.upperEndpoint());
        }
        queryBuilder.filter().add(rqb);
        return;
      }
    }

    queryBuilder
        .filter()
        .add(
            QueryBuilders.termQuery(
                getExactMatchOrVerbatimField(predicate),
                parseParamValue(predicate.getValue(), parameter)));
  }

  private Function<String, Object> parseDate =
      d -> "*".equals(d) ? null : IsoDateParsingUtils.parseDate(d);

  private Function<String, Object> parseInteger = d -> "*".equals(d) ? null : Integer.parseInt(d);

  private Function<String, Object> parseDouble = d -> "*".equals(d) ? null : Double.parseDouble(d);

  public void visit(RangePredicate<S> predicate, BoolQueryBuilder queryBuilder)
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

    queryBuilder.filter().add(rqb);
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
   * @param queryBuilder root query builder
   */
  public void visit(GreaterThanOrEqualsPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();
    queryBuilder
        .filter()
        .add(
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .gte(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles greater than predicate
   *
   * @param predicate greater than predicate
   * @param queryBuilder root query builder
   */
  public void visit(GreaterThanPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();
    queryBuilder
        .filter()
        .add(
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .gt(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   * @param queryBuilder root query builder
   */
  public void visit(InPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();
    queryBuilder
        .filter()
        .add(
            QueryBuilders.termsQuery(
                getExactMatchOrVerbatimField(predicate),
                predicate.getValues().stream()
                    .map(v -> parseParamValue(v, parameter))
                    .collect(Collectors.toList())));
  }

  /**
   * handles less than or equals predicate
   *
   * @param predicate less than or equals
   * @param queryBuilder root query builder
   */
  public void visit(LessThanOrEqualsPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();
    queryBuilder
        .filter()
        .add(
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .lte(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles less than predicate
   *
   * @param predicate less than predicate
   * @param queryBuilder root query builder
   */
  public void visit(LessThanPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    S parameter = predicate.getKey();
    queryBuilder
        .filter()
        .add(
            QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(parameter))
                .lt(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   * @param queryBuilder root query builder
   */
  public void visit(LikePredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder
        .filter()
        .add(
            QueryBuilders.wildcardQuery(
                getExactMatchOrVerbatimField(predicate), predicate.getValue()));
  }

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   * @param queryBuilder root query builder
   */
  public void visit(NotPredicate predicate, BoolQueryBuilder queryBuilder)
      throws QueryBuildingException {
    BoolQueryBuilder mustNotQueryBuilder = QueryBuilders.boolQuery();
    visit(predicate.getPredicate(), mustNotQueryBuilder);
    queryBuilder.mustNot(mustNotQueryBuilder);
  }

  /**
   * handles within predicate
   *
   * @param within Within predicate
   * @param queryBuilder root query builder
   */
  public void visit(WithinPredicate within, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter(buildGeoShapeQuery(within.getGeometry()));
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
   * @param queryBuilder root query builder
   */
  public void visit(GeoDistancePredicate geoDistance, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter(buildGeoDistanceQuery(geoDistance.getGeoDistance()));
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
  public void visit(IsNotNullPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder
        .filter()
        .add(
            QueryBuilders.existsQuery(
                esFieldMapper.getExactMatchFieldName(predicate.getParameter())));
  }

  /**
   * handles ISNULL Predicate
   *
   * @param predicate ISNULL predicate
   */
  public void visit(IsNullPredicate<S> predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder
        .filter()
        .add(
            QueryBuilders.boolQuery()
                .mustNot(
                    QueryBuilders.existsQuery(
                        esFieldMapper.getExactMatchFieldName(predicate.getParameter()))));
  }

  private void visit(Object object, BoolQueryBuilder queryBuilder) throws QueryBuildingException {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass(), BoolQueryBuilder.class);
    } catch (NoSuchMethodException e) {
      log.warn(
          "Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object, queryBuilder);
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
}
