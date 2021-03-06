/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.api.model.predicate;

import static org.gbif.api.model.predicate.PreconditionUtils.checkArgument;

import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.gbif.api.annotation.Experimental;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.util.SearchTypeValidator;

public class SimplePredicate<S extends SearchParameter> implements Predicate {

  @NotNull private final S key;

  @NotNull private final String value;

  @Nullable private final Boolean matchCase;

  protected SimplePredicate(boolean checkForNonEquals, S key, String value, Boolean matchCase) {
    this.matchCase = matchCase;
    Objects.requireNonNull(key, "<key> may not be null");
    Objects.requireNonNull(value, "<value> may not be null");
    checkArgument(!value.isEmpty(), "<value> may not be empty");
    // make sure the value is of the right type according to the key given
    SearchTypeValidator.validate(key, value);

    this.key = key;
    this.value = value;

    checkPredicateAllowed();
    if (checkForNonEquals) {
      checkNonEqualsComparatorAllowed();
    }
  }

  public S getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  /**
   * This flag enables the use of case-sensitive matches and aggregations on certain search
   * parameters.
   *
   * <p>Fields that support this feature are: occurrenceId, recordedBy, samplingProtocol,
   * catalogNumber, collectionCode, institutionCode, eventId, parentEventId, waterBody,
   * stateProvince, recordNumber, identifiedBy, organismId and locality.
   *
   * <p>This is an experimental feature and its implementation map change or be removed at any time.
   */
  @Experimental
  public Boolean isMatchCase() {
    return Optional.ofNullable(matchCase).orElse(Boolean.FALSE);
  }

  /** @throws IllegalArgumentException if the key SearchParameter is Geometry */
  private void checkPredicateAllowed() {
    if ("GEOMETRY".equals(key.name())) {
      throw new IllegalArgumentException("Geometry parameter must use a Within predicate");
    }
  }

  /**
   * @throws IllegalArgumentException if the key SearchParameter allows other comparators than
   *     equals
   */
  private void checkNonEqualsComparatorAllowed() {
    if (!(Number.class.isAssignableFrom(key.type()) || Date.class.isAssignableFrom(key.type()))) {
      throw new IllegalArgumentException(
          "Only equals comparisons are allowed for search parameter " + key);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimplePredicate<S> that = (SimplePredicate<S>) o;
    return key == that.key && Objects.equals(value, that.value) && matchCase == that.matchCase;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SimplePredicate.class.getSimpleName() + "[", "]")
        .add("key=" + key)
        .add("value='" + value + "'")
        .add("matchCase='" + matchCase + "'")
        .toString();
  }
}
