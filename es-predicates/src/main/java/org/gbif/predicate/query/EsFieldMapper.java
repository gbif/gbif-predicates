package org.gbif.predicate.query;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import java.util.List;
import java.util.Optional;
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

  List<SortOptions> getDefaultSort();

  default Optional<Query> getDefaultFilter() {
    return Optional.empty();
  }

  default boolean isNestedField(P searchParameter) {
    String path = getNestedPath(searchParameter);
    return path != null && !path.isBlank();
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
  default boolean includeNullInRange(P param, RangeQuery rangeQuery) {
    return false;
  }

  EsField getEsField(P parameter);

  EsField getEsFacetField(P parameter);

  boolean isDateField(EsField esField);

  EsField getGeoDistanceEsField();

  EsField getGeoShapeEsField();
}
