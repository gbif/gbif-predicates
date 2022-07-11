package org.gbif.predicate.query;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.Term;

/** Interface to encapsulate search parameter conversions to terms. */
public interface SQLTermsMapper<S extends SearchParameter> {

  Term term(S searchParameter);

  boolean isArray(S searchParameter);

  Term getTermArray(S searchParameter);

  boolean isDenormedTerm(S searchParameter);

  S getDefaultGadmLevel();
}
