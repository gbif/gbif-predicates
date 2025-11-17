package org.gbif.predicate.query;

import java.util.List;
import java.util.Optional;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.SimplePredicate;

public interface EsFieldMapper<P extends SearchParameter> {

  String getVerbatimFieldName(P searchParameter);

  String getExactMatchFieldName(P searchParameter);

  String getGeoDistanceField();

  String getGeoShapeField();

  String getSearchFieldName(P searchParameter);

  boolean isVocabulary(P searchParameter);

  String getChecklistField(String checklistKey, P searchParameter);

  String getNestedPath(P searchParameter);

  String getFullTextField();

  List<FieldSortBuilder> getDefaultSort();

  default Optional<QueryBuilder> getDefaultFilter() {
    return Optional.empty();
  }

  default boolean isNestedField(P searchParameter) {
    return !Strings.isNullOrEmpty(getNestedPath(searchParameter));
  }

  /**
   * Returns true if the search parameter is taxonomic related and hence will be determined by which
   * checklist is in use.
   *
   * @param searchParameter
   * @return true if a taxonomic parameter
   */
  boolean isTaxonomic(P searchParameter);

  /**
   * Adds an "is null" filter if the mapper instructs to do it for the specific predicate. Used
   * mostly in range queries to give specific semantics to null values.
   */
  default boolean includeNullInPredicate(SimplePredicate<P> predicate) {
    return false;
  }

  /**
   * Adds an "is null" filter if the mapper instructs to do it for the range query. Used mostly in
   * range queries to give specific semantics to null values.
   */
  default boolean includeNullInRange(P param, RangeQueryBuilder rangeQueryBuilder) {
    return false;
  }

  EsField getEsField(P parameter);

  EsField getEsFacetField(P parameter);

  boolean isDateField(EsField esField);

  EsField getGeoDistanceEsField();

  EsField getGeoShapeEsField();
}
