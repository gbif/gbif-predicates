package org.gbif.predicate.query;

import org.gbif.api.model.common.search.SearchParameter;

public interface EsFieldMapper<S extends SearchParameter> {

  String getVerbatimFieldName(S searchParameter);

  String getExactMatchFieldName(S searchParameter);

  String getGeoDistanceField();

  String getGeoShapeField();

  boolean isVocabulary(S searchParameter);
}
