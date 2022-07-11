package org.gbif.api.model.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.File;
import java.io.IOException;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test cases and examples of serialization or predicates using a mixing. */
public class PredicateDeSerTest {

  @JsonDeserialize(as = OccurrenceSearchParameter.class)
  public class OccurrenceSearchParameterMixin {}

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.addMixIn(SearchParameter.class, OccurrenceSearchParameterMixin.class);
  }

  private File getTestFile(String predicateFile) {
    return new File(getClass().getResource("/predicate/" + predicateFile).getFile());
  }

  @Test
  public void deserTest() throws IOException {
    assertPredicate("is_null.json");
    assertPredicate("conjuction.json");
    assertPredicate("within.json");
    assertPredicate("equals_catalog_number.json");
    assertPredicate("like_catalog_number.json");
    assertPredicate("and_with_not.json");
    assertPredicate("conjunction_with_in.json");
    assertPredicate("complex_conjunction_with_in.json");
  }

  private void assertPredicate(String fileName) throws IOException {
    Predicate predicate = MAPPER.readValue(getTestFile(fileName), Predicate.class);
    Assertions.assertNotNull(predicate);
  }
}
