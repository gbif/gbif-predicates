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

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

public class NotPredicateTest {

  @Test
  public void testBasics() {
    Predicate p = mock(Predicate.class);

    org.gbif.api.model.predicate.NotPredicate np = new NotPredicate(p);
    assertThat(p, equalTo(np.getPredicate()));
  }

  @Test
  public void testEquals() {
    Predicate p = mock(Predicate.class);

    Predicate np1 = new NotPredicate(p);
    Predicate np2 = new NotPredicate(p);

    assertThat(np1, both(equalTo(np1)).and(equalTo(np2)));

    np2 = new NotPredicate(mock(Predicate.class));
    assertThat(np1, not(equalTo(np2)));

    assertThat(p, not(equalTo(np1)));
  }

  @Test
  public void testHashcode() {
    Predicate p = mock(Predicate.class);

    Predicate np1 = new NotPredicate(p);
    Predicate np2 = new NotPredicate(p);

    assertThat(np1.hashCode(), both(equalTo(np1.hashCode())).and(equalTo(np2.hashCode())));

    np2 = new NotPredicate(mock(Predicate.class));
    assertThat(np1.hashCode(), not(equalTo(np2.hashCode())));

    assertThat(p.hashCode(), not(equalTo(np1.hashCode())));
  }

  @Test
  public void testNullConstructor() {
    assertThrows(NullPointerException.class, () -> new NotPredicate(null));
  }
}
