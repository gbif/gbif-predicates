package org.gbif.predicate.query;

import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.Term;

/** Interface to encapsulate search parameter conversions to terms. */
public interface SQLTermsMapper<S extends SearchParameter> {

  Map<S, ? extends Term> getParam2Terms();

  Map<S, Term> getArrayTerms();

  Map<S, Term> getDenormedTerms();

  String getSqlColumn(Term term);
}
