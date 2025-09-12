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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GeoDistancePredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.IsNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanPredicate;
import org.gbif.api.model.predicate.LikePredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.RangePredicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.Range;
import org.gbif.api.util.RangeValue;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;
import org.gbif.predicate.query.occurrence.OccurrenceTermsMapper;
import org.junit.jupiter.api.Test;

public class SQLQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 =
      OccurrenceSearchParameter.INSTITUTION_CODE;
  private final SQLQueryVisitor visitor = new SQLQueryVisitor(new OccurrenceTermsMapper());

  @Test
  public void testComplexQuery() throws QueryBuildingException {
    Predicate aves = new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "212", false);
    Predicate passer =
        new LikePredicate<>(OccurrenceSearchParameter.SCIENTIFIC_NAME, "Passer*", false);
    Predicate UK = new EqualsPredicate<>(OccurrenceSearchParameter.COUNTRY, "GB", false);
    Predicate before1989 = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, "1989");
    Predicate georeferencedPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.HAS_COORDINATE, "true", false);

    ConjunctionPredicate p =
        new ConjunctionPredicate(
            Lists.newArrayList(aves, UK, passer, before1989, georeferencedPredicate));
    String where = visitor.buildQuery(p);
    assertEquals(
        "(((taxonkey = 212 OR acceptedtaxonkey = 212 OR kingdomkey = 212 OR phylumkey = 212 OR classkey = 212 OR orderkey = 212 OR familykey = 212 OR genuskey = 212 OR specieskey = 212)) AND (countrycode = \'GB\') AND (lower(scientificname) LIKE lower(\'Passer%\')) AND (year <= 1989) AND (hascoordinate = true))",
        where);
  }

  @Test
  public void testMoreComplexQuery() throws QueryBuildingException {
    Predicate taxon1 = new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "1", false);
    Predicate taxon2 = new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "2", false);
    DisjunctionPredicate taxa = new DisjunctionPredicate(Lists.newArrayList(taxon1, taxon2));

    Predicate basis =
        new InPredicate<>(
            OccurrenceSearchParameter.BASIS_OF_RECORD,
            Lists.newArrayList("HUMAN_OBSERVATION", "MACHINE_OBSERVATION"),
            false);

    Predicate UK = new EqualsPredicate<>(OccurrenceSearchParameter.COUNTRY, "GB", false);
    Predicate IE = new EqualsPredicate<>(OccurrenceSearchParameter.COUNTRY, "IE", false);
    DisjunctionPredicate countries = new DisjunctionPredicate(Lists.newArrayList(UK, IE));

    Predicate before1989 = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, "1989");
    Predicate in2000 = new EqualsPredicate<>(OccurrenceSearchParameter.YEAR, "2000", false);
    DisjunctionPredicate years = new DisjunctionPredicate(Lists.newArrayList(before1989, in2000));

    ConjunctionPredicate p =
        new ConjunctionPredicate(Lists.newArrayList(taxa, basis, countries, years));
    String where = visitor.buildQuery(p);
    assertEquals(
        "(((taxonkey IN('1', '2') OR acceptedtaxonkey IN('1', '2') OR kingdomkey IN('1', '2') OR phylumkey IN('1', '2') OR classkey IN('1', '2') OR orderkey IN('1', '2') OR familykey IN('1', '2') OR genuskey IN('1', '2') OR specieskey IN('1', '2'))) "
            + "AND ((basisofrecord IN('HUMAN_OBSERVATION', 'MACHINE_OBSERVATION'))) "
            + "AND ((countrycode IN(\'GB\', \'IE\'))) "
            + "AND (((year <= 1989) OR (year = 2000))))",
        where);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals(
        "((lower(catalognumber) = lower(\'value_1\')) AND (lower(institutioncode) = lower(\'value_2\')))",
        query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals(
        "((lower(catalognumber) = lower(\'value_1\')) OR (lower(institutioncode) = lower(\'value_2\')))",
        query);
  }

  @Test
  public void testDisjunctionToInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM, "value_2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals("(lower(catalognumber) IN(lower(\'value_1\'), lower(\'value_2\')))", query);
  }

  @Test
  public void testDisjunctionToInTaxonPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "1", false);
    Predicate p2 = new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals(
        "(taxonkey IN('1', '2') OR acceptedtaxonkey IN('1', '2') OR kingdomkey IN('1', '2') OR phylumkey IN('1', '2') OR classkey IN('1', '2') OR orderkey IN('1', '2') OR familykey IN('1', '2') OR genuskey IN('1', '2') OR specieskey IN('1', '2'))",
        query);
  }

  @Test
  public void testDisjunctionToInGadmGidPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(OccurrenceSearchParameter.GADM_GID, "IRL_1", false);
    Predicate p2 = new EqualsPredicate<>(OccurrenceSearchParameter.GADM_GID, "GBR.2_1", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals(
        "(level0gid IN('IRL_1', 'GBR.2_1') OR level1gid IN('IRL_1', 'GBR.2_1') OR level2gid IN('IRL_1', 'GBR.2_1') OR level3gid IN('IRL_1', 'GBR.2_1'))",
        query);
  }

  @Test
  public void testDisjunctionMediaTypePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(OccurrenceSearchParameter.MEDIA_TYPE, "StillImage", false);
    Predicate p2 = new EqualsPredicate<>(OccurrenceSearchParameter.MEDIA_TYPE, "Sound", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals(
        "(stringArrayContains(mediatype,'StillImage',true) OR stringArrayContains(mediatype,'Sound',true))",
        query);
  }

  @Test
  public void testDisjunctionVerbatimToInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", true);
    Predicate p2 = new EqualsPredicate<>(PARAM, "value_2", true);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.buildQuery(p);
    assertEquals("(catalognumber IN('value_1', 'value_2'))", query);

    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", false);

    p = new DisjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    query = visitor.buildQuery(p);
    assertEquals(
        "((catalognumber = 'value_1') OR (catalognumber = 'value_2') OR (lower(catalognumber) = lower('value_3')))",
        query);
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", false);
    String query = visitor.buildQuery(p);
    assertEquals("lower(catalognumber) = lower(\'value\')", query);
  }

  @Test
  public void testEqualsPredicateGadmGid() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.GADM_GID, "IRL_1", false);

    String query = visitor.buildQuery(p);
    assertEquals(
        "(level0gid = 'IRL_1' OR level1gid = 'IRL_1' OR level2gid = 'IRL_1' OR level3gid = 'IRL_1')",
        query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    // NB: ? and * are wildcards (translated to SQL _ and %), so literal _ and % are escaped.
    Predicate p = new LikePredicate<>(PARAM, "v?l*ue_%", false);
    String query = visitor.buildQuery(p);
    assertEquals("lower(catalognumber) LIKE lower(\'v_l%ue\\_\\%\')", query);
  }

  @Test
  public void testLikeVerbatimPredicate() throws QueryBuildingException {
    Predicate p = new LikePredicate<>(PARAM, "v?l*ue_%", true);
    String query = visitor.buildQuery(p);
    assertEquals("catalognumber LIKE \'v_l%ue\\_\\%\'", query);
  }

  @Test
  public void testEqualsVerbatimPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", true);
    String query = visitor.buildQuery(p);
    assertEquals("catalognumber = \'value\'", query);
  }

  @Test
  public void testEqualsArrayPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.RECORDED_BY, "value", false);
    String query = visitor.buildQuery(p);
    assertEquals("stringArrayContains(recordedby,'value',false)", query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.RECORDED_BY, "value", true);
    query = visitor.buildQuery(p);
    assertEquals("stringArrayContains(recordedby,'value',true)", query);
  }

  @Test
  public void testLikeArrayPredicate() throws QueryBuildingException {
    // NB: ? and * are the wildcards here.
    Predicate p = new LikePredicate<>(OccurrenceSearchParameter.RECORDED_BY, "v?l*ue_%", false);
    String query = visitor.buildQuery(p);
    assertEquals("stringArrayLike(recordedby,'v?l*ue_%',false)", query);

    p = new LikePredicate<>(OccurrenceSearchParameter.RECORDED_BY, "v?l*ue_%", true);
    query = visitor.buildQuery(p);
    assertEquals("stringArrayLike(recordedby,'v?l*ue_%',true)", query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.buildQuery(p);
    assertEquals("elevation >= 222", query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    assertEquals("elevation > 1000", query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"), false);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(lower(catalognumber) IN(lower(\'value_1\'), lower(\'value_2\'), lower(\'value_3\')))",
        query);
  }

  @Test
  public void testInVerbatimPredicate() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"), true);
    String query = visitor.buildQuery(p);
    assertEquals("(catalognumber IN(\'value_1\', \'value_2\', \'value_3\'))", query);
  }

  @Test
  public void testInPredicateTaxonKey() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(OccurrenceSearchParameter.TAXON_KEY, Lists.newArrayList("1", "2"), false);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(taxonkey IN('1', '2') OR acceptedtaxonkey IN('1', '2') OR kingdomkey IN('1', '2') OR phylumkey IN('1', '2') OR classkey IN('1', '2') OR orderkey IN('1', '2') OR familykey IN('1', '2') OR genuskey IN('1', '2') OR specieskey IN('1', '2'))",
        query);
  }

  @Test
  public void testInPredicateGadmGid() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(
            OccurrenceSearchParameter.GADM_GID, Lists.newArrayList("IRL_1", "GBR.2_1"), false);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(level0gid IN('IRL_1', 'GBR.2_1') OR level1gid IN('IRL_1', 'GBR.2_1') OR level2gid IN('IRL_1', 'GBR.2_1') OR level3gid IN('IRL_1', 'GBR.2_1'))",
        query);
  }

  @Test
  public void testInPredicateMediaType() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(
            OccurrenceSearchParameter.MEDIA_TYPE, Lists.newArrayList("StillImage", "Sound"), false);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(stringArrayContains(mediatype,'StillImage',true) OR stringArrayContains(mediatype,'Sound',true))",
        query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    assertEquals("elevation <= 1000", query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.buildQuery(p);
    assertEquals("elevation < 1000", query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate<>(PARAM, "value", false));
    String query = visitor.buildQuery(p);
    assertEquals("NOT lower(catalognumber) = lower(\'value\')", query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.buildQuery(p);
    assertEquals(
        "NOT ((lower(catalognumber) = lower(\'value_1\')) AND (lower(institutioncode) = lower(\'value_2\')))",
        query);
  }

  @Test
  public void testQuotes() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "my \'pleasure\'", false);
    String query = visitor.buildQuery(p);
    assertEquals("lower(catalognumber) = lower(\'my \\\'pleasure\\\'\')", query);

    Predicate predicateRecordedBY =
        new EqualsPredicate<>(OccurrenceSearchParameter.RECORDED_BY, "Brian J O'Shea", false);
    String queryRecordedBy = visitor.buildQuery(predicateRecordedBY);
    assertEquals("stringArrayContains(recordedby,'Brian J O\\'Shea',false)", queryRecordedBy);

    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "101");
    query = visitor.buildQuery(p);
    assertEquals("elevation <= 101", query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.YEAR, "1998");
    query = visitor.buildQuery(p);
    assertEquals("year > 1998", query);
  }

  @Test
  public void testGeoDistancePredicate() throws QueryBuildingException {
    Predicate p = new GeoDistancePredicate("30", "10", "10km");
    String query = visitor.buildQuery(p);
    assertEquals(
        "(geoDistance(30.0, 10.0, '10.0km', decimallatitude, decimallongitude) = TRUE)", query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.buildQuery(p);
    assertEquals("(contains('" + wkt + "', decimallatitude, decimallongitude) = TRUE)", query);
  }

  @Test
  public void testLongerWithinPredicate() throws QueryBuildingException {
    final String wkt =
        "POLYGON ((-21.4671921 65.441761, -21.3157028 65.9990267, -22.46732 66.4657148, -23.196803 66.3490242, -22.362113 66.2703732, -22.9758561 66.228119, -22.3831844 66.0933255, -22.424131 65.8374539, -23.4703372 66.1972321, -23.2565264 65.6767322, -24.5319933 65.5027259, -21.684764 65.4547893, -24.0482947 64.8794291, -21.3551366 64.3842337, -22.7053151 63.8001572, -19.1269971 63.3980322, -13.4948065 65.076438, -15.1872897 66.1073781, -14.5302343 66.3783121, -16.0235596 66.5371808, -21.4671921 65.441761))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.buildQuery(p);
    assertEquals(
        "((decimallatitude >= 63.3980322 AND decimallatitude <= 66.5371808 AND (decimallongitude >= -24.5319933 AND decimallongitude <= -13.4948065)) AND contains('"
            + wkt
            + "', decimallatitude, decimallongitude) = TRUE)",
        query);
  }

  @Test
  public void testAntimeridianWithinPredicate() throws Exception {
    // A rectangle over the Bering sea, shouldn't have any bounding box added
    String wkt =
        "POLYGON((-206.71875 39.20502, -133.59375 39.20502, -133.59375 77.26611, -206.71875 77.26611, -206.71875 39.20502))";
    String query = visitor.buildQuery(new WithinPredicate(wkt));
    assertEquals("(contains('" + wkt + "', decimallatitude, decimallongitude) = TRUE)", query);
  }

  @Test
  public void testAddedBoundingBoxes() throws Exception {
    String query;

    // A multipolygon around Taveuni, split over the antimeridian
    String wktM =
        "MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))";
    String bbox =
        "(decimallatitude >= -17.12845 AND decimallatitude <= -16.60277 AND (decimallongitude >= 179.78577 OR decimallongitude <= -179.75006))";
    query = visitor.buildQuery(new WithinPredicate(wktM));
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // A polygon around Taveuni, Fiji, as portal16 produces it.
    // Note the result still contains the multipolygon.
    String wkt16 =
        "POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))";
    query = visitor.buildQuery(new WithinPredicate(wkt16));
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // Same place, but as Wicket draws it:
    // Note the result still contains the same multipolygon.
    String wktWk =
        "POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))";
    query = visitor.buildQuery(new WithinPredicate(wktWk));
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // Tiny areas scattered around the world, all in a single multipolygon.
    // Requires bounding boxes to avoid very slow Hive performance.
    String wktMM =
        "MULTIPOLYGON (((-109.42861 -27.13333, -109.42861 -27.13666, -109.43138 -27.13666, -109.43138 -27.13333, -109.42861 -27.13333)), "
            + "((-109.42236 -27.10919, -109.42236 -27.11191, -109.42541 -27.11191, -109.42541 -27.10919, -109.42236 -27.10919)), "
            + "((-174.35146 -19.80528, -174.35146 -19.81829, -174.36338 -19.81829, -174.36338 -19.80528, -174.35146 -19.80528)), "
            + "((-173.94525 -18.71444, -173.96472 -18.71444, -173.96472 -18.68694, -173.94525 -18.68694, -173.94525 -18.71444)), "
            + "((-173.91655 -18.66372, -173.94041 -18.66372, -173.94041 -18.63616, -173.91655 -18.63616, -173.91655 -18.66372)))";
    String bboxMM =
        "(decimallatitude >= -27.13666 AND decimallatitude <= -18.63616 AND (decimallongitude >= -174.36338 AND decimallongitude <= -109.42236)) AND "
            + "(((decimallatitude >= -27.13666 AND decimallatitude <= -27.13333 AND (decimallongitude >= -109.43138 AND decimallongitude <= -109.42861)) OR "
            + "(decimallatitude >= -27.11191 AND decimallatitude <= -27.10919 AND (decimallongitude >= -109.42541 AND decimallongitude <= -109.42236)) OR "
            + "(decimallatitude >= -19.81829 AND decimallatitude <= -19.80528 AND (decimallongitude >= -174.36338 AND decimallongitude <= -174.35146)) OR "
            + "(decimallatitude >= -18.71444 AND decimallatitude <= -18.68694 AND (decimallongitude >= -173.96472 AND decimallongitude <= -173.94525)) OR "
            + "(decimallatitude >= -18.66372 AND decimallatitude <= -18.63616 AND (decimallongitude >= -173.94041 AND decimallongitude <= -173.91655))))";
    query = visitor.buildQuery(new WithinPredicate(wktMM));
    assertEquals(
        "(" + bboxMM + " AND contains('" + wktMM + "', decimallatitude, decimallongitude) = TRUE)",
        query);
  }

  @Test
  public void testWidePolygons() throws Exception {
    String query;

    // A polygon around Antarctica
    String wktP =
        "POLYGON ((180 -64.7, 180 -56.8, 180 -44.3, 173 -44.3, 173 -47.5, 170 -47.5, 157 -47.5, 157 -45.9, 150 -45.9, 150 -47.5, 143 -47.5, 143 -45.8, 140 -45.8, 140 -44.5, 137 -44.5, 137 -43, 135 -43, 135 -41.7, 131 -41.7, 131 -40.1, 115 -40.1, 92 -40.1, 92 -41.4, 78 -41.4, 78 -42.3, 69 -42.3, 69 -43.3, 47 -43.3, 47 -41.7, 30 -41.7, 12 -41.7, 12 -40.3, 10 -40.3, 10 -38.3, -5 -38.3, -5 -38.9, -9 -38.9, -9 -40.2, -13 -40.2, -13 -41.4, -21 -41.4, -21 -42.5, -39 -42.5, -39 -40.7, -49 -40.7, -49 -48.6, -54 -48.6, -54 -55.7, -62.79726 -55.7, -64 -55.7, -64 -57.8, -71 -57.8, -71 -58.9, -80 -58.9, -80 -40, -103.71094 -40.14844, -125 -40, -167 -40, -167 -42.6, -171 -42.6, -171 -44.3, -180 -44.3, -180 -56.8, -180 -64.7, -180 -80, -125 -80, -70 -80, 30 -80, 115 -80, 158 -80, 180 -80, 180 -64.7))";
    query = visitor.buildQuery(new WithinPredicate(wktP));
    assertEquals(
        "((decimallatitude >= -80.0 AND decimallatitude <= -38.3 AND (decimallongitude >= -180.0 AND decimallongitude <= 180.0)) AND contains('"
            + wktP
            + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // A multipolygon around the Pacific and Indian oceans, split over the antimeridian
    String wktM =
        "MULTIPOLYGON (((180 51.83076923076923, 180 -63, 35 -63, 60 -9, 127 1, 157 49, 180 51.83076923076923)), ((-180 -63, -180 51.83076923076923, -138 57, -127 39, -112 18, -92 13, -84 1, -77 -63, -169 -63, -180 -63)))";
    String bbox =
        "(decimallatitude >= -63.0 AND decimallatitude <= 57.0 AND (decimallongitude >= 35.0 OR decimallongitude <= -77.0))";
    query = visitor.buildQuery(new WithinPredicate(wktM));
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // The same polygon, as portal16 produces it.
    // Note the result still contains the multipolygon.
    String wkt16 =
        "POLYGON((35.0 -63.0, 191.0 -63.0, 283.0 -63.0, 276.0 1.0, 268.0 13.0, 248.0 18.0, 233.0 39.0, 222.0 57.0, 157.0 49.0, 127.0 1.0, 60.0 -9.0, 35.0 -63.0))";
    query = visitor.buildQuery(new WithinPredicate(wkt16));
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);

    // A polygon around the Pacific, as Wicket draws it:
    String wktWk =
        "POLYGON((157.0 49.0,127.0 1.0,60.0 -9.0,35.0 -63.0,-169.0 -63.0,-77.0 -63.0,-84.0 1.0,-92.0 13.0,-112.0 18.0,-127.0 39.0,-138.0 57.0,157.0 49.0))";
    assertEquals(
        "(" + bbox + " AND contains('" + wktM + "', decimallatitude, decimallongitude) = TRUE)",
        query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    assertEquals("catalognumber IS NOT NULL", query);
  }

  @Test
  public void testIsArrayNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(OccurrenceSearchParameter.IDENTIFIED_BY_ID);
    String query = visitor.buildQuery(p);
    assertEquals("(identifiedbyid IS NOT NULL AND size(identifiedbyid) > 0)", query);
  }

  @Test
  public void testIsVocabularyNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(OccurrenceSearchParameter.LIFE_STAGE);
    String query = visitor.buildQuery(p);
    assertEquals("(lifestage.lineage IS NOT NULL AND size(lifestage.lineage) > 0)", query);
  }

  @Test
  public void testIsNotNullPredicateGadmGid() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(OccurrenceSearchParameter.GADM_GID);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(level0gid IS NOT NULL AND level1gid IS NOT NULL AND level2gid IS NOT NULL AND level3gid IS NOT NULL)",
        query);
  }

  @Test
  public void testIsNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(PARAM);
    String query = visitor.buildQuery(p);
    assertEquals("catalognumber IS NULL", query);
  }

  @Test
  public void testIsNullPredicateGadmGid() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(OccurrenceSearchParameter.GADM_GID);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(level0gid IS NULL AND level1gid IS NULL AND level2gid IS NULL AND level3gid IS NULL)",
        query);
  }

  @Test
  public void testIsArrayNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(OccurrenceSearchParameter.IDENTIFIED_BY_ID);
    String query = visitor.buildQuery(p);
    assertEquals("(identifiedbyid IS NULL OR size(identifiedbyid) = 0)", query);
  }

  @Test
  public void testIsVocabularyNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(OccurrenceSearchParameter.LIFE_STAGE);
    String query = visitor.buildQuery(p);
    assertEquals("lifestage.lineage IS NULL", query);
  }

  @Test
  public void testIsNotNullTaxonKey() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(OccurrenceSearchParameter.TAXON_KEY);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(taxonkey IS NOT NULL AND acceptedtaxonkey IS NOT NULL AND kingdomkey IS NOT NULL AND phylumkey IS NOT NULL AND classkey IS NOT NULL AND orderkey IS NOT NULL AND familykey IS NOT NULL AND genuskey IS NOT NULL AND specieskey IS NOT NULL)",
        query);
  }

  @Test
  public void testIsNullTaxonKey() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(OccurrenceSearchParameter.TAXON_KEY);
    String query = visitor.buildQuery(p);
    assertEquals(
        "(taxonkey IS NULL AND acceptedtaxonkey IS NULL AND kingdomkey IS NULL AND phylumkey IS NULL AND classkey IS NULL AND orderkey IS NULL AND familykey IS NULL AND genuskey IS NULL AND specieskey IS NULL)",
        query);
  }

  @Test
  public void testIsNotNullArrayPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(OccurrenceSearchParameter.MEDIA_TYPE);
    String query = visitor.buildQuery(p);
    assertEquals("(mediatype IS NOT NULL AND size(mediatype) > 0)", query);
  }

  @Test
  public void testPartialDates() throws QueryBuildingException {
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-26T00:00:00Z", "2021-10-25");
    testPartialDate("2014-10-01T00:00:00Z", "2014-11-01T00:00:00Z", "2014-10");
    testPartialDate("1936-01-01T00:00:00Z", "1937-01-01T00:00:00Z", "1936");
  }

  @Test
  public void testDateRanges() throws QueryBuildingException {
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-26T00:00:00Z", "2021-10-25,2021-10-25");
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-27T00:00:00Z", "2021-10-25,2021-10-26");
    testPartialDate("2014-05-01T00:00:00Z", "2014-07-01T00:00:00Z", "2014-05,2014-06");
    testPartialDate("1936-01-01T00:00:00Z", "1941-01-01T00:00:00Z", "1936,1940");
    testPartialDate("1940-01-01T00:00:00Z", null, "1940,*");
    testPartialDate(null, "1941-01-01T00:00:00Z", "*,1940");
    testPartialDate(null, null, "*,*");
  }

  @Test
  public void testDateComparisons() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000", false);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(lastinterpreted >= %s AND lastinterpreted < %s)",
            Instant.parse("2000-01-01T00:00:00Z").toEpochMilli(),
            Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()),
        query);

    // Include the range
    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("lastinterpreted < %s", Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()),
        query);

    p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "lastinterpreted >= %s", Instant.parse("2000-01-01T00:00:00Z").toEpochMilli()),
        query);

    // Exclude the range
    p = new LessThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("lastinterpreted < %s", Instant.parse("2000-01-01T00:00:00Z").toEpochMilli()),
        query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "lastinterpreted >= %s", Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()),
        query);
  }

  @Test
  public void testDateRangeComparisons() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000-01-02", false);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-03T00:00:00Z").getEpochSecond()),
        query);

    p = new InPredicate<>(OccurrenceSearchParameter.EVENT_DATE, Arrays.asList("2000-01-02"), false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((eventdategte >= %s AND eventdatelte < %s))",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-03T00:00:00Z").getEpochSecond()),
        query);

    p =
        new InPredicate<>(
            OccurrenceSearchParameter.EVENT_DATE, Arrays.asList("2000-01-02", "2024-01-17"), false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((eventdategte >= %s AND eventdatelte < %s) OR (eventdategte >= %s AND eventdatelte < %s))",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-03T00:00:00Z").getEpochSecond(),
            Instant.parse("2024-01-17T00:00:00Z").getEpochSecond(),
            Instant.parse("2024-01-18T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000-01-02,2000-01-04", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-05T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000-01", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-01T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-02-01T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000-01,2000-03", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-01T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-04-01T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-01T00:00:00Z").getEpochSecond(),
            Instant.parse("2001-01-01T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000,2001", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-01T00:00:00Z").getEpochSecond(),
            Instant.parse("2002-01-01T00:00:00Z").getEpochSecond()),
        query);

    // Include the range (for the or-equal-to)
    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("eventdategte < %s", Instant.parse("2001-01-01T00:00:00Z").getEpochSecond()),
        query);

    p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("eventdatelte >= %s", Instant.parse("2000-01-01T00:00:00Z").getEpochSecond()),
        query);

    // Exclude the range
    p = new LessThanPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("eventdategte < %s", Instant.parse("2000-01-01T00:00:00Z").getEpochSecond()),
        query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000");
    query = visitor.buildQuery(p);
    assertEquals(
        String.format("eventdatelte >= %s", Instant.parse("2001-01-01T00:00:00Z").getEpochSecond()),
        query);
  }

  /** Reusable method to test partial dates, i.e., dates with the format: yyyy, yyyy-MM. */
  private void testPartialDate(String expectedFrom, String expectedTo, String value)
      throws QueryBuildingException {
    Range<Instant> range =
        Range.closed(
            expectedFrom == null ? null : Instant.parse(expectedFrom),
            expectedTo == null ? null : Instant.parse(expectedTo));

    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, value, false);
    String query = visitor.buildQuery(p);

    if (!range.hasUpperBound() && !range.hasLowerBound()) {
      assertEquals("", query);
    } else if (!range.hasUpperBound()) {
      assertEquals(
          String.format("(lastinterpreted >= %s)", range.lowerEndpoint().toEpochMilli()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(
          String.format("(lastinterpreted < %s)", range.upperEndpoint().toEpochMilli()), query);
    } else {
      assertEquals(
          String.format(
              "(lastinterpreted >= %s AND lastinterpreted < %s)",
              range.lowerEndpoint().toEpochMilli(), range.upperEndpoint().toEpochMilli()),
          query);
    }
  }

  /** Some dates in HDFS are a number of seconds, others a number of milliseconds. */
  @Test
  public void testDateMagnitude() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2000-01-02", false);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(eventdategte >= %s AND eventdatelte < %s)",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-03T00:00:00Z").getEpochSecond()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2000-01-02", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(lastinterpreted >= %s AND lastinterpreted < %s)",
            Instant.parse("2000-01-02T00:00:00Z").toEpochMilli(),
            Instant.parse("2000-01-03T00:00:00Z").toEpochMilli()),
        query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.MODIFIED, "2000-01-02", false);
    query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "(modified >= %s AND modified < %s)",
            Instant.parse("2000-01-02T00:00:00Z").getEpochSecond(),
            Instant.parse("2000-01-03T00:00:00Z").getEpochSecond()),
        query);
  }

  @Test
  public void testIntRanges() throws QueryBuildingException {

    String value = "1990,2011";
    Range<Integer> range = SearchTypeValidator.parseIntegerRange(value);

    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.YEAR, value, false);
    String query = visitor.buildQuery(p);

    if (!range.hasUpperBound()) {
      assertEquals(String.format("year >= %s", range.lowerEndpoint().intValue()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(String.format("year <= %s", range.upperEndpoint().intValue()), query);
    } else {
      assertEquals(
          String.format(
              "((year >= %s) AND (year <= %s))",
              range.lowerEndpoint().intValue(), range.upperEndpoint().intValue()),
          query);
    }
  }

  @Test
  public void testIntInclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue("1990", null, "2011", null);
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((year >= %1$s) AND (year <= %2$s))",
            Integer.parseInt(rangeValue.getGte()), Integer.parseInt(rangeValue.getLte())),
        query);
  }

  @Test
  public void testIntExclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue(null, "1990", null, "2011");
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((year > %1$s) AND (year < %2$s))",
            Integer.parseInt(rangeValue.getGt()), Integer.parseInt(rangeValue.getLt())),
        query);
  }

  @Test
  public void testInclusiveExclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue("1990", null, null, "2011");
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((year >= %1$s) AND (year < %2$s))",
            Integer.parseInt(rangeValue.getGte()), Integer.parseInt(rangeValue.getLt())),
        query);
  }

  @Test
  public void testExclusiveInclusiveRangeWithRangePredicate() throws QueryBuildingException {

    RangeValue rangeValue = new RangeValue(null, "1990", "2011", null);
    Predicate p = new RangePredicate(OccurrenceSearchParameter.YEAR, rangeValue);
    String query = visitor.buildQuery(p);
    assertEquals(
        String.format(
            "((year > %1$s) AND (year <= %2$s))",
            Integer.parseInt(rangeValue.getGt()), Integer.parseInt(rangeValue.getLte())),
        query);
  }

  @Test
  public void testDoubleRanges() throws QueryBuildingException {
    testDoubleRange("-200,600.2");
    testDoubleRange("*,300.3");
    testDoubleRange("-23.8,*");
  }

  /** Reusable method to test number ranges. */
  private void testDoubleRange(String value) throws QueryBuildingException {
    Range<Double> range = SearchTypeValidator.parseDecimalRange(value);

    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, value, false);
    String query = visitor.buildQuery(p);

    if (!range.hasUpperBound()) {
      assertEquals(String.format("elevation >= %s", range.lowerEndpoint().doubleValue()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(String.format("elevation <= %s", range.upperEndpoint().doubleValue()), query);
    } else {
      assertEquals(
          String.format(
              "((elevation >= %s) AND (elevation <= %s))",
              range.lowerEndpoint().doubleValue(), range.upperEndpoint().doubleValue()),
          query);
    }
  }

  @Test
  public void testIntegerRanges() throws QueryBuildingException {
    testIntegerRange("1950,1960");
    testIntegerRange("*,2000");
    testIntegerRange("2000,*");
  }

  /** Reusable method to test number ranges. */
  private void testIntegerRange(String value) throws QueryBuildingException {
    Range<Integer> range = SearchTypeValidator.parseIntegerRange(value);

    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.YEAR, value, false);
    String query = visitor.buildQuery(p);

    if (!range.hasUpperBound()) {
      assertEquals(String.format("year >= %s", range.lowerEndpoint().intValue()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(String.format("year <= %s", range.upperEndpoint().intValue()), query);
    } else {
      assertEquals(
          String.format(
              "((year >= %s) AND (year <= %s))",
              range.lowerEndpoint().intValue(), range.upperEndpoint().intValue()),
          query);
    }
  }

  @Test
  public void testIssues() throws QueryBuildingException {
    // EqualsPredicate
    String query =
        visitor.buildQuery(
            new EqualsPredicate<>(
                OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false));
    assertEquals("stringArrayContains(issue,'TAXON_MATCH_HIGHERRANK',true)", query);

    // InPredicate
    query =
        visitor.buildQuery(
            new InPredicate<>(
                OccurrenceSearchParameter.ISSUE,
                Lists.newArrayList("TAXON_MATCH_HIGHERRANK", "TAXON_MATCH_NONE"),
                false));
    assertEquals(
        "(stringArrayContains(issue,'TAXON_MATCH_HIGHERRANK',true) OR stringArrayContains(issue,'TAXON_MATCH_NONE',true))",
        query);

    // LikePredicate
    try {
      new LikePredicate<>(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false);
      fail();
    } catch (IllegalArgumentException e) {
    }

    // Not
    query =
        visitor.buildQuery(
            new NotPredicate(
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false)));
    assertEquals("NOT stringArrayContains(issue,'TAXON_MATCH_HIGHERRANK',true)", query);

    // Not disjunction
    query =
        visitor.buildQuery(
            new NotPredicate(
                new DisjunctionPredicate(
                    Lists.newArrayList(
                        new EqualsPredicate<>(
                            OccurrenceSearchParameter.ISSUE, "COORDINATE_INVALID", false),
                        new EqualsPredicate<>(
                            OccurrenceSearchParameter.ISSUE, "COORDINATE_OUT_OF_RANGE", false),
                        new EqualsPredicate<>(
                            OccurrenceSearchParameter.ISSUE, "ZERO_COORDINATE", false),
                        new EqualsPredicate<>(
                            OccurrenceSearchParameter.ISSUE, "RECORDED_DATE_INVALID", false)))));
    assertEquals(
        "NOT (stringArrayContains(issue,'COORDINATE_INVALID',true) OR stringArrayContains(issue,'COORDINATE_OUT_OF_RANGE',true) OR stringArrayContains(issue,'ZERO_COORDINATE',true) OR stringArrayContains(issue,'RECORDED_DATE_INVALID',true))",
        query);

    // IsNotNull
    query = visitor.buildQuery(new IsNotNullPredicate<>(OccurrenceSearchParameter.ISSUE));
    assertEquals("(issue IS NOT NULL AND size(issue) > 0)", query);
  }

  @Test
  public void testAllParamsExist() {
    List<Predicate> predicates = Lists.newArrayList();
    for (OccurrenceSearchParameter param : OccurrenceSearchParameter.values()) {
      if (param == OccurrenceSearchParameter.EVENT_ID_HIERARCHY
          || param == OccurrenceSearchParameter.EVENT_TYPE
          || param == OccurrenceSearchParameter.VERBATIM_EVENT_TYPE
          || param.name().startsWith("HUMBOLDT_")) {
        // skip events-only parameters
        continue;
      }

      String value = "7";

      if (OccurrenceSearchParameter.GEOMETRY == param) {
        value = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
        predicates.add(new WithinPredicate(value));
      } else if (UUID.class.isAssignableFrom(param.type())) {
        value = UUID.randomUUID().toString();
      } else if (Boolean.class.isAssignableFrom(param.type())) {
        value = "true";
      } else if (Country.class.isAssignableFrom(param.type())) {
        value = Country.GERMANY.getIso2LetterCode();
      } else if (Language.class.isAssignableFrom(param.type())) {
        value = Language.GERMAN.getIso2LetterCode();
      } else if (Enum.class.isAssignableFrom(param.type())) {
        Enum<?>[] values = ((Class<Enum>) param.type()).getEnumConstants();
        value = values[0].name();
      } else if (Date.class.isAssignableFrom(param.type())) {
        value = "2014-01-23";
      } else if (IsoDateInterval.class.isAssignableFrom(param.type())) {
        value = "2014-01-23,2015-05";
      } else if (OccurrenceSearchParameter.GEO_DISTANCE == param) {
        predicates.add(new GeoDistancePredicate("10", "20", "10km"));
      }

      if (OccurrenceSearchParameter.GEOMETRY != param
          && OccurrenceSearchParameter.GEO_DISTANCE != param) {
        predicates.add(new EqualsPredicate<>(param, value, false));
      }
    }
    ConjunctionPredicate and = new ConjunctionPredicate(predicates);
    try {
      visitor.buildQuery(and);
    } catch (QueryBuildingException e) {
      fail(e);
    }
  }

  @Test
  public void testVocabularies() {
    Arrays.stream(OccurrenceSearchParameter.values())
        .filter(
            p ->
                Optional.ofNullable(visitor.term(p))
                    .map(SQLColumnsUtils::isVocabulary)
                    .orElse(false))
        .forEach(
            param -> {
              try {
                String hiveQueryField = SQLColumnsUtils.getSQLQueryColumn(visitor.term(param));

                // EqualsPredicate
                String query = visitor.buildQuery(new EqualsPredicate<>(param, "value_1", false));
                assertEquals("stringArrayContains(" + hiveQueryField + ",'value_1',false)", query);

                // InPredicate
                query =
                    visitor.buildQuery(
                        new InPredicate<>(param, Lists.newArrayList("value_1", "value_2"), false));
                assertEquals(
                    "(stringArrayContains("
                        + hiveQueryField
                        + ",'value_1',false) OR stringArrayContains("
                        + hiveQueryField
                        + ",'value_2',false))",
                    query);

                // LikePredicate
                query = visitor.buildQuery(new LikePredicate<>(param, "value_*", false));
                assertEquals("lower(" + hiveQueryField + ") LIKE lower('value\\_%')", query);

                // Not
                query =
                    visitor.buildQuery(
                        new NotPredicate(new EqualsPredicate<>(param, "value_1", false)));
                assertEquals(
                    "NOT stringArrayContains(" + hiveQueryField + ",'value_1',false)", query);

                // IsNotNull
                query = visitor.buildQuery(new IsNotNullPredicate<>(param));
                assertEquals(
                    "(" + hiveQueryField + " IS NOT NULL AND size(" + hiveQueryField + ") > 0)",
                    query);

                // IsNull
                query = visitor.buildQuery(new IsNullPredicate<>(param));
                if (visitor.isSQLArray(param)) {
                  assertEquals(
                      " (" + hiveQueryField + " IS NULL OR size(" + hiveQueryField + ") = 0) ",
                      query);
                } else {
                  assertEquals(hiveQueryField + " IS NULL", query);
                }

              } catch (QueryBuildingException ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  @Test
  public void testGreaterThanEqualsIncludingNull() {
    GreaterThanOrEqualsPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    try {
      String query = visitor.buildQuery(distanceFromCentroidPredicate);
      assertEquals(
          "(distancefromcentroidinmeters >= 10 OR distancefromcentroidinmeters IS NULL)", query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testGreaterThanIncludingNull() {
    GreaterThanPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    try {
      String query = visitor.buildQuery(distanceFromCentroidPredicate);
      assertEquals(
          "(distancefromcentroidinmeters > 10 OR distancefromcentroidinmeters IS NULL)", query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testDisjunctionGreaterThanEqualsIncludingNull() {
    GreaterThanOrEqualsPredicate<OccurrenceSearchParameter> distanceFromCentroidPredicate =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, "10");
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "6", false);
    DisjunctionPredicate disjunctionPredicate =
        new DisjunctionPredicate(Arrays.asList(equalsPredicate, distanceFromCentroidPredicate));
    try {
      String query = visitor.buildQuery(disjunctionPredicate);
      assertEquals(
          "(((taxonkey = 6 OR acceptedtaxonkey = 6 OR kingdomkey = 6 OR phylumkey = 6 OR classkey = 6 OR orderkey = 6 OR familykey = 6 OR genuskey = 6 OR specieskey = 6)) OR ((distancefromcentroidinmeters >= 10 OR distancefromcentroidinmeters IS NULL)))",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testGeoTimePredicate() throws QueryBuildingException {
    Predicate predicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.GEOLOGICAL_TIME, "12", false);
    String query = visitor.buildQuery(predicate);
    assertEquals("12 > geologicaltime.gt AND 12 <= geologicaltime.lte", query);

    Predicate rangePredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.GEOLOGICAL_TIME, "12,15", false);
    query = visitor.buildQuery(rangePredicate);
    assertEquals("geologicaltime.gt >= 12.0 AND geologicaltime.lte <= 15.0", query);

    rangePredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.GEOLOGICAL_TIME, "12,*", false);
    query = visitor.buildQuery(rangePredicate);
    assertEquals("geologicaltime.gt >= 12.0", query);

    rangePredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.GEOLOGICAL_TIME, "*,15", false);
    query = visitor.buildQuery(rangePredicate);
    assertEquals("geologicaltime.lte <= 15.0", query);
  }

  @Test
  public void testMultiTaxonomyEqualsPredicate() {
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "6", false, "my-checklist-uuid");
    try {
      String query = visitor.buildQuery(equalsPredicate);
      assertEquals("(stringArrayContains(classifications['my-checklist-uuid'], '6', true))", query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testMultiTaxonomyInPredicate() {
    InPredicate<OccurrenceSearchParameter> inPredicate =
        new InPredicate<>(
            OccurrenceSearchParameter.TAXON_KEY, List.of("6", "7"), false, "my-checklist-uuid");
    try {
      String query = visitor.buildQuery(inPredicate);
      assertEquals(
          "((stringArrayContains(classifications['my-checklist-uuid'], '6', true)) OR (stringArrayContains(classifications['my-checklist-uuid'], '7', true)))",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testMultiTaxonomyDisjunctionPredicate() {
    DisjunctionPredicate predicate =
        new DisjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY, "6", false, "my-checklist-uuid-1"),
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY, "7", false, "my-checklist-uuid-2")));
    try {
      String query = visitor.buildQuery(predicate);
      assertEquals(
          "(((stringArrayContains(classifications['my-checklist-uuid-1'], '6', true))) OR ((stringArrayContains(classifications['my-checklist-uuid-2'], '7', true))))",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testMultiTaxonomyConjunctionPredicate() {
    ConjunctionPredicate predicate =
        new ConjunctionPredicate(
            Arrays.asList(
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY, "6", false, "my-checklist-uuid-1"),
                new EqualsPredicate<>(
                    OccurrenceSearchParameter.TAXON_KEY, "7", false, "my-checklist-uuid-2")));
    try {
      String query = visitor.buildQuery(predicate);
      assertEquals(
          "(((stringArrayContains(classifications['my-checklist-uuid-1'], '6', true))) AND ((stringArrayContains(classifications['my-checklist-uuid-2'], '7', true))))",
          query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }

  @Test
  public void testHumboldtAlias() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.HUMBOLDT_SITE_COUNT, "1", false);
    String query = visitor.buildQuery(p);
    assertEquals("h.sitecount = 1", query);
  }

  @Test
  public void testHumboldtEventDuration() throws QueryBuildingException {
    Predicate p =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES, "1", false);
    String query = visitor.buildQuery(p);
    assertEquals("h.humboldteventdurationvalueinminutes = 1", query);

    p =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES, "1,3", false);
    query = visitor.buildQuery(p);
    assertEquals(
        "((h.humboldteventdurationvalueinminutes >= 1.0) AND (h.humboldteventdurationvalueinminutes <= 3.0))",
        query);

    p =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES, "1,*", false);
    query = visitor.buildQuery(p);
    assertEquals("h.humboldteventdurationvalueinminutes >= 1.0", query);
  }

  @Test
  public void testHumboldtTaxonomyEqualsPredicate() throws QueryBuildingException {
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, "6", false, "def");
    String query = visitor.buildQuery(equalsPredicate);
    assertEquals(
        "(stringArrayContains(humboldttargettaxonclassifications['def']['taxonkeys'], '6', true))",
        query);

    equalsPredicate =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, "6", false, "def");
    query = visitor.buildQuery(equalsPredicate);
    assertEquals(
        "(stringArrayContains(humboldttargettaxonclassifications['def']['usagekey'], '6', true))",
        query);
  }

  @Test
  public void testHumboldtFields() {
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, "1", false);
    try {
      String query = visitor.buildQuery(equalsPredicate);
      assertEquals("lower(h.verbatimsitenames) = lower('1')", query);
    } catch (QueryBuildingException ex) {
      fail();
    }

    equalsPredicate =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, "true", false, "def");
    try {
      String query = visitor.buildQuery(equalsPredicate);
      assertEquals("h.isabsencereported = true", query);
    } catch (QueryBuildingException ex) {
      fail();
    }
  }
}
