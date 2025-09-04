package org.gbif.predicate.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.SimplePredicate;

public interface EsFieldMapper<S extends SearchParameter> {

  String getVerbatimFieldName(S searchParameter);

  String getExactMatchFieldName(S searchParameter);

  String getGeoDistanceField();

  String getGeoShapeField();

  boolean isVocabulary(S searchParameter);

  String getChecklistField(String checklistKey, S searchParameter);

  String getNestedPath(S searchParameter);

  default boolean isNestedField(S searchParameter) {
    return !Strings.isNullOrEmpty(getNestedPath(searchParameter));
  }

  /**
   * Returns true if the search parameter is taxonomic related and hence will be determined by which
   * checklist is in use.
   *
   * @param searchParameter
   * @return true if a taxonomic parameter
   */
  boolean isTaxonomic(S searchParameter);
  /**
   * Adds an "is null" filter if the mapper instructs to do it for the specific predicate. Used
   * mostly in range queries to give specific semantics to null values.
   */
  default boolean includeNullInPredicate(SimplePredicate<S> predicate) {
    return false;
  }

  /**
   * Adds an "is null" filter if the mapper instructs to do it for the range query. Used mostly in
   * range queries to give specific semantics to null values.
   */
  default boolean includeNullInRange(S param, RangeQueryBuilder rangeQueryBuilder) {
    return false;
  }
}
