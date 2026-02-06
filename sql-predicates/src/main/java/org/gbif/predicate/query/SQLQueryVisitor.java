package org.gbif.predicate.query;

import static org.gbif.api.util.IsoDateParsingUtils.ISO_DATE_FORMATTER;
import static org.gbif.predicate.query.SQLColumnsUtils.HUMBOLDT_TAXON_COLUMNS;
import static org.gbif.predicate.query.SQLColumnsUtils.isInterpretedUtcDateMilliseconds;

import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.occurrence.search.InternalOccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.CompoundPredicate;
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
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.query.QueryVisitor;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.*;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

@RequiredArgsConstructor
@Slf4j
public class SQLQueryVisitor<S extends SearchParameter> implements QueryVisitor {

  private static final String CONJUNCTION_OPERATOR = " AND ";
  private static final String DISJUNCTION_OPERATOR = " OR ";
  private static final String EQUALS_OPERATOR = " = ";
  private static final String IN_OPERATOR = " IN";
  private static final String GREATER_THAN_OPERATOR = " > ";
  private static final String GREATER_THAN_EQUALS_OPERATOR = " >= ";
  private static final String LESS_THAN_OPERATOR = " < ";
  private static final String LESS_THAN_EQUALS_OPERATOR = " <= ";
  private static final String NOT_OPERATOR = "NOT ";
  private static final String LIKE_OPERATOR = " LIKE ";
  private static final String IS_NOT_NULL_OPERATOR = " IS NOT NULL";
  private static final String IS_NULL_OPERATOR = " IS NULL";
  private static final String IS_NOT_NULL_ARRAY_OPERATOR = "(%1$s IS NOT NULL AND size(%1$s) > 0)";
  private static final String IS_NULL_ARRAY_OPERATOR = "(%1$s IS NULL OR size(%1$s) = 0)";
  // where query to execute a select all
  private static final String ALL_QUERY = "true";

  private static final String SQL_ARRAY_PRE = "ARRAY";

  private static final Function<Term, String> ARRAY_FN =
      t -> "stringArrayContains(" + SQLColumnsUtils.getSQLQueryColumn(t) + ",'%s',%b)";

  private static final Function<Term, String> ARRAY_LIKE_FN =
      t -> "stringArrayLike(" + SQLColumnsUtils.getSQLQueryColumn(t) + ",'%s',%b)";

  // TODO: handle derived taxon params for events
  private static final Set<OccurrenceSearchParameter> TAXON_SEARCH_PARAMETERS =
      Set.of(
          OccurrenceSearchParameter.KINGDOM_KEY,
          OccurrenceSearchParameter.PHYLUM_KEY,
          OccurrenceSearchParameter.CLASS_KEY,
          OccurrenceSearchParameter.ORDER_KEY,
          OccurrenceSearchParameter.FAMILY_KEY,
          OccurrenceSearchParameter.SUBGENUS_KEY,
          OccurrenceSearchParameter.GENUS_KEY,
          OccurrenceSearchParameter.SPECIES_KEY,
          OccurrenceSearchParameter.TAXON_KEY,
          OccurrenceSearchParameter.ACCEPTED_TAXON_KEY);

  private static final List<GadmTerm> GADM_GIDS =
      List.of(GadmTerm.level0Gid, GadmTerm.level1Gid, GadmTerm.level2Gid, GadmTerm.level3Gid);

  private StringBuilder builder;

  private final SQLTermsMapper<S> sqlTermsMapper;

  private final String defaultChecklistKey;

  /** Transforms the value to the SQL statement lower(val). */
  protected String toSQLLower(String val) {
    return "lower(" + val + ")";
  }

  protected String toSQLField(S param, boolean matchCase) {
    return Optional.ofNullable(term(param))
        .map(
            term -> {
              String sqlCol = SQLColumnsUtils.getSQLQueryColumn(term);
              if (String.class.isAssignableFrom(param.type())
                  && (param != OccurrenceSearchParameter.GEOMETRY)
                  && !matchCase) {
                return toSQLLower(sqlCol);
              }
              return sqlCol;
            })
        .orElseThrow(
            () ->
                // QueryBuildingException requires an underlying exception
                new IllegalArgumentException(
                    "Search parameter " + param.type().getName() + "." + param + " is not mapped"));
  }

  /**
   * Allow support for querying the denormalized extension.
   *
   * <p>FIXME - this can probably be factored out once all required fields are denormalized.
   *
   * @param param
   * @param matchCase
   * @return
   */
  protected String toSQLDenormField(S param, boolean matchCase) {
    return Optional.ofNullable(term(param))
        .map(
            term -> {
              if (term instanceof DwcTerm) {
                DwcTerm dwcTerm = (DwcTerm) term;
                String field = "Denorm.parents." + dwcTerm.simpleName();
                if (String.class.isAssignableFrom(param.type())
                    && !"GEOMETRY".equals(param.name())
                    && !matchCase) {
                  return toSQLLower(field);
                }
                return field;
              }
              return null;
            })
        .orElseThrow(
            () ->
                // QueryBuildingException requires an underlying exception
                new IllegalArgumentException(
                    "Search parameter " + param + " is not mapped to Hive"));
  }

  /**
   * Converts a value to the form expected by Hive based on the SearchParameter. Most values pass by
   * unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
   *
   * @param param the type of parameter defining the expected type
   * @param value the original query value
   * @return the converted value expected by Hive
   */
  protected String toSQLValue(S param, String value, boolean matchCase) {
    if (Enum.class.isAssignableFrom(param.type())) {
      // all enum parameters are uppercase
      return '\'' + value.toUpperCase() + '\'';
    }

    if (Date.class.isAssignableFrom(param.type())) {
      // use longs for timestamps expressed as ISO dates
      LocalDate ld = IsoDateParsingUtils.parseDate(value);
      Instant i = ld.atStartOfDay(ZoneOffset.UTC).toInstant();
      if (isInterpretedUtcDateMilliseconds(term(param))) {
        return String.valueOf(i.toEpochMilli());
      } else {
        return String.valueOf(i.getEpochSecond());
      }

    } else if (Number.class.isAssignableFrom(param.type())
        || Boolean.class.isAssignableFrom(param.type())) {
      // do not quote numbers
      return value;

    } else {
      // quote value, escape existing quotes
      String strVal = '\'' + value.replaceAll("'", "\\\\'") + '\'';
      if (String.class.isAssignableFrom(param.type())
          && !"GEOMETRY".equals(param.name())
          && !matchCase) {
        return toSQLLower(strVal);
      }
      return strVal;
    }
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into
   * a strings that can be used as the <em>WHERE</em> clause for a SQL (hive or spark) download.
   *
   * @param predicate to translate
   * @return WHERE clause
   */
  public String buildQuery(Predicate predicate) throws QueryBuildingException {
    String query = ALL_QUERY;
    if (predicate != null) { // null predicate means a SELECT ALL
      builder = new StringBuilder();
      visit(predicate);
      query = builder.toString();
    }

    // Set to null to prevent old StringBuilders hanging around in case this class is reused
    // somewhere else
    builder = null;
    return query;
  }

  public void visit(ConjunctionPredicate predicate) throws QueryBuildingException {
    visitCompoundPredicate(predicate, CONJUNCTION_OPERATOR);
  }

  public void visit(DisjunctionPredicate predicate) throws QueryBuildingException {
    // See if this disjunction can be simplified into an IN predicate, which is much faster.
    // We could overcomplicate this:
    //   A=1 OR A=2 OR B=3 OR B=4 OR C>5 → A IN(1,2) OR B IN (3,4) OR C>5
    // but that's a very unusual query for us, so we just check for
    // - EqualsPredicates everywhere
    // - on the same search parameter.

    boolean useIn = true;
    Boolean matchCase = null;
    List<String> values = new ArrayList<>();
    S parameter = null;
    String checklistsKey = null;

    for (Predicate subPredicate : predicate.getPredicates()) {
      if (subPredicate instanceof EqualsPredicate) {
        EqualsPredicate<S> equalsSubPredicate = (EqualsPredicate<S>) subPredicate;
        if (parameter == null) {
          parameter = equalsSubPredicate.getKey();
          matchCase = equalsSubPredicate.isMatchCase();
          checklistsKey = getChecklistKey(equalsSubPredicate.getChecklistKey());
        } else if (parameter != equalsSubPredicate.getKey()
            || matchCase != equalsSubPredicate.isMatchCase()
            || !Objects.equals(
                checklistsKey, getChecklistKey(equalsSubPredicate.getChecklistKey()))) {
          useIn = false;
          break;
        }
        values.add(equalsSubPredicate.getValue());
      } else {
        useIn = false;
        break;
      }
    }

    if (useIn) {
      visit(new InPredicate<>(parameter, values, matchCase));
    } else {
      visitCompoundPredicate(predicate, DISJUNCTION_OPERATOR);
    }
  }

  public Function<Term, String> getArrayFn() {
    return ARRAY_FN;
  }

  /** Supports all parameters incl taxonKey expansion for higher taxa. */
  public void visit(EqualsPredicate<S> predicate) throws QueryBuildingException {
    if (isHumboldtTaxonParameter(predicate.getKey())) {
      appendHumboldtTaxonFilter(predicate);
    } else if (TAXON_SEARCH_PARAMETERS.contains(predicate.getKey())) {
      appendTaxonomicArrayFilter(predicate, GbifInternalTerm.classifications);
    } else if (predicate.getKey() == OccurrenceSearchParameter.TAXONOMIC_ISSUE) {
      appendTaxonomicArrayFilter(predicate, GbifTerm.taxonomicIssue);
    } else if (predicate.getKey() == OccurrenceSearchParameter.TAXONOMIC_STATUS) {
      appendTaxonomicSingleValueFilter(predicate, GbifInternalTerm.taxonomicStatuses);
    } else if (predicate.getKey() == OccurrenceSearchParameter.GADM_GID) {
      appendGadmFilterList(GADM_GIDS, predicate.getValue());
    } else if (predicate.getKey() == OccurrenceSearchParameter.MEDIA_TYPE) {
      Optional.ofNullable(VocabularyUtils.lookupEnum(predicate.getValue(), MediaType.class))
          .ifPresent(
              mediaType ->
                  builder.append(
                      String.format(
                          getArrayFn().apply(GbifTerm.mediaType), mediaType.name(), true)));
    } else if (predicate.getKey() == OccurrenceSearchParameter.ISSUE) {
      builder.append(
          String.format(
              getArrayFn().apply(GbifTerm.issue), predicate.getValue().toUpperCase(), true));
    } else if (sqlTermsMapper.isArray(predicate.getKey())) {
      builder.append(
          String.format(
              getArrayFn().apply(sqlTermsMapper.getTermArray(predicate.getKey())),
              predicate.getValue().replaceAll("'", "\\\\'"),
              predicate.isMatchCase()));
    } else if (sqlTermsMapper.isDenormedTerm(predicate.getKey())) {
      builder
          .append("(")
          .append("(")
          .append(toSQLField(predicate.getKey(), predicate.isMatchCase()))
          .append(EQUALS_OPERATOR)
          .append(toSQLValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()))
          .append(") OR (")
          .append("stringArrayContains(")
          .append(toSQLDenormField(predicate.getKey(), true))
          .append(",")
          .append(toSQLValue(predicate.getKey(), predicate.getValue(), true))
          .append("))")
          .append(")");
    } else if (SQLColumnsUtils.isVocabulary(term(predicate.getKey()))) {
      builder.append(
          String.format(getArrayFn().apply(term(predicate.getKey())), predicate.getValue(), false));
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Dates may contain a range even for an EqualsPredicate (e.g. "2000" or "2000-02")
      // The user's query value is inclusive, but the parsed dateRange is exclusive of the
      // upperBound to allow including the day itself.
      //
      // I.e. a predicate value 2000/2005-03 gives a dateRange [2000-01-01,2005-04-01)
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());

      if (dateRange.hasLowerBound() || dateRange.hasUpperBound()) {
        builder.append('(');
        if (dateRange.hasLowerBound()) {
          visitSimplePredicate(
              predicate,
              GREATER_THAN_EQUALS_OPERATOR,
              ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
          if (dateRange.hasUpperBound()) {
            builder.append(CONJUNCTION_OPERATOR);
          }
        }
        if (dateRange.hasUpperBound()) {
          visitSimplePredicate(
              predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
        }
        builder.append(')');
      }
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      // Dates may contain a range even for an EqualsPredicate (e.g. "2000" or "2000-02")
      // The user's query value is inclusive, but the parsed dateRange is exclusive of the
      // upperBound to allow including the day itself.
      //
      // I.e. a predicate value 2000/2005-03 gives a dateRange [2000-01-01,2005-04-01)
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());

      if (dateRange.hasLowerBound() || dateRange.hasUpperBound()) {
        // all the stuff here
        builder.append('(');
        if (dateRange.hasLowerBound()) {
          SimplePredicate predicateGte =
              new EqualsPredicate<>(
                  InternalOccurrenceSearchParameter.EVENT_DATE_GTE,
                  predicate.getValue(),
                  predicate.isMatchCase());
          visitSimplePredicate(
              predicateGte,
              GREATER_THAN_EQUALS_OPERATOR,
              ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
          if (dateRange.hasUpperBound()) {
            builder.append(CONJUNCTION_OPERATOR);
          }
        }
        if (dateRange.hasUpperBound()) {
          SimplePredicate predicateLte =
              new EqualsPredicate<>(
                  InternalOccurrenceSearchParameter.EVENT_DATE_LTE,
                  predicate.getValue(),
                  predicate.isMatchCase());
          visitSimplePredicate(
              predicateLte,
              LESS_THAN_OPERATOR,
              ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
        }
        builder.append(')');
      }
    } else if (predicate.getKey() == OccurrenceSearchParameter.GEOLOGICAL_TIME) {
      if (SearchTypeValidator.isNumericRange(predicate.getValue())) {
        Range<Double> range = SearchTypeValidator.parseDecimalRange(predicate.getValue());
        if (range.hasLowerBound()) {
          builder
              .append("geologicaltime.gt")
              .append(GREATER_THAN_EQUALS_OPERATOR)
              .append(range.lowerEndpoint());
        }
        if (range.hasUpperBound()) {
          if (range.hasLowerBound()) {
            builder.append(CONJUNCTION_OPERATOR);
          }
          builder
              .append("geologicaltime.lte")
              .append(LESS_THAN_EQUALS_OPERATOR)
              .append(range.upperEndpoint());
        }
      } else {
        builder
            .append(predicate.getValue())
            .append(GREATER_THAN_OPERATOR)
            .append("geologicaltime.gt")
            .append(CONJUNCTION_OPERATOR)
            .append(predicate.getValue())
            .append(LESS_THAN_EQUALS_OPERATOR)
            .append("geologicaltime.lte");
      }
    } else {
      visitSimplePredicate(predicate, EQUALS_OPERATOR);
    }
  }

  public void visit(GreaterThanOrEqualsPredicate<S> predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(
          predicate,
          GREATER_THAN_EQUALS_OPERATOR,
          ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      SimplePredicate predicateLte =
          new GreaterThanOrEqualsPredicate<>(
              InternalOccurrenceSearchParameter.EVENT_DATE_LTE, predicate.getValue());
      visitSimplePredicate(
          predicateLte,
          GREATER_THAN_EQUALS_OPERATOR,
          ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else {
      visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR);
    }
  }

  public void visit(GreaterThanPredicate<S> predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole
      // range.
      // "2000" excludes all of 2000, so the earliest date is 2001-01-01.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(
          predicate,
          GREATER_THAN_EQUALS_OPERATOR,
          ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole
      // range.
      // "2000" excludes all of 2000, so the earliest date is 2001-01-01.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      SimplePredicate predicateLte =
          new GreaterThanPredicate<>(
              InternalOccurrenceSearchParameter.EVENT_DATE_LTE, predicate.getValue());
      visitSimplePredicate(
          predicateLte,
          GREATER_THAN_EQUALS_OPERATOR,
          ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));

    } else {
      visitSimplePredicate(predicate, GREATER_THAN_OPERATOR);
    }
  }

  public void visit(LessThanOrEqualsPredicate<S> predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000, so the latest date is 2001-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(
          predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000, so the latest date is 2001-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      SimplePredicate predicateGte =
          new LessThanOrEqualsPredicate<>(
              InternalOccurrenceSearchParameter.EVENT_DATE_GTE, predicate.getValue());
      visitSimplePredicate(
          predicateGte, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
    } else {
      visitSimplePredicate(predicate, LESS_THAN_EQUALS_OPERATOR);
    }
  }

  public void visit(LessThanPredicate<S> predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole
      // range.
      // "2000" excludes all of 2000, so the latest date is 2000-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(
          predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else if (IsoDateInterval.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole
      // range.
      // "2000" excludes all of 2000, so the latest date is 2000-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      SimplePredicate predicateGte =
          new LessThanPredicate<>(
              InternalOccurrenceSearchParameter.EVENT_DATE_GTE, predicate.getValue());
      visitSimplePredicate(
          predicateGte, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else {
      visitSimplePredicate(predicate, LESS_THAN_OPERATOR);
    }
  }

  public void visit(RangePredicate<S> predicate) throws QueryBuildingException {
    builder.append("((");

    if (!Objects.isNull(predicate.getValue().getGte())) {
      visitSimplePredicate(
          new LessThanOrEqualsPredicate<>(predicate.getKey(), predicate.getValue().getGte()),
          GREATER_THAN_EQUALS_OPERATOR);
    } else {
      visitSimplePredicate(
          new LessThanPredicate<>(predicate.getKey(), predicate.getValue().getGt()),
          GREATER_THAN_OPERATOR);
    }

    builder.append(")");
    builder.append(CONJUNCTION_OPERATOR);
    builder.append("(");

    if (!Objects.isNull(predicate.getValue().getLte())) {
      visitSimplePredicate(
          new GreaterThanOrEqualsPredicate<>(predicate.getKey(), predicate.getValue().getLte()),
          LESS_THAN_EQUALS_OPERATOR);
    } else {
      visitSimplePredicate(
          new GreaterThanOrEqualsPredicate<>(predicate.getKey(), predicate.getValue().getLt()),
          LESS_THAN_OPERATOR);
    }
    builder.append("))");
  }

  /*
   * For large disjunctions, IN predicates are around 3× faster than a perfectly balanced tree of
   * OR predicates, and around 2× faster than a fairly flat OR query.
   *
   * With Hive 1.3.0, balancing OR queries should be internal to Hive:
   *   https://jira.apache.org/jira/browse/HIVE-11398
   * but it is probably still better to use an IN, which uses a hash table lookup internally:
   *   https://jira.apache.org/jira/browse/HIVE-11415#comment-14651085
   */
  public void visit(InPredicate<S> predicate) throws QueryBuildingException {

    log.info("InPredicate " + predicate);

    boolean isMatchCase = Optional.ofNullable(predicate.isMatchCase()).orElse(Boolean.FALSE);

    if (isSQLArray(predicate.getKey()) || SQLColumnsUtils.isVocabulary(term(predicate.getKey()))) {
      // Array values must be converted to ORs.
      builder.append('(');
      Iterator<String> iterator = predicate.getValues().iterator();
      while (iterator.hasNext()) {
        // Use the equals predicate to get the behaviour for array.
        visit(new EqualsPredicate<S>(predicate.getKey(), iterator.next(), isMatchCase));
        if (iterator.hasNext()) {
          builder.append(DISJUNCTION_OPERATOR);
        }
      }
      builder.append(')');
    } else if (isHumboldtTaxonParameter(predicate.getKey())) {
      appendHumboldtTaxonFilter(predicate);
    } else if (TAXON_SEARCH_PARAMETERS.contains((OccurrenceSearchParameter) predicate.getKey())) {
      appendTaxonomicArrayFilter(predicate, GbifInternalTerm.classifications);
    } else if (predicate.getKey() == OccurrenceSearchParameter.TAXONOMIC_ISSUE) {
      appendTaxonomicArrayFilter(predicate, GbifTerm.taxonomicIssue);
    } else if (predicate.getKey() == OccurrenceSearchParameter.TAXONOMIC_STATUS) {
      appendTaxonomicSingleValueFilter(predicate, GbifInternalTerm.taxonomicStatuses);
    } else if (predicate.getKey().name().equals("GADM_GID")) {
      // GADM GIDs must be expanded into a disjunction of in predicates
      appendGadmGidFilter(predicate.getValues());
    } else if (predicate.getKey().name().equals("EVENT_DATE")) {
      // Event dates must be expanded into a disjunction of conjunction predicates (of comparisons)
      builder.append('(');
      Iterator<String> iterator = predicate.getValues().iterator();
      while (iterator.hasNext()) {
        // Use the equals predicate to get the behaviour for event dates.
        visit(new EqualsPredicate<S>(predicate.getKey(), iterator.next(), isMatchCase));
        if (iterator.hasNext()) {
          builder.append(DISJUNCTION_OPERATOR);
        }
      }
      builder.append(')');

    } else {
      builder
          .append('(')
          .append(toSQLField(predicate.getKey(), isMatchCase))
          .append(IN_OPERATOR)
          .append('(');
      Iterator<String> iterator = predicate.getValues().iterator();
      while (iterator.hasNext()) {
        builder.append(toSQLValue(predicate.getKey(), iterator.next(), isMatchCase));
        if (iterator.hasNext()) {
          builder.append(", ");
        }
      }
      builder.append(")");

      // this block can be removed in future if we don't need a denormalized
      // AVRO extension
      if (sqlTermsMapper.isDenormedTerm(predicate.getKey())) {
        builder.append(" OR ").append('(');

        Iterator<String> iterator2 = predicate.getValues().iterator();
        while (iterator2.hasNext()) {
          builder
              .append('(')
              // FIX ME
              .append("array_contains(")
              .append(toSQLDenormField(predicate.getKey(), true))
              .append(',')
              .append(toSQLValue(predicate.getKey(), iterator2.next(), true))
              .append(')')
              .append(')');
          if (iterator2.hasNext()) {
            builder.append(" OR ");
          }
        }
        builder.append(")");
      }
      builder.append(")");
    }
  }

  public void visit(LikePredicate<S> predicate) throws QueryBuildingException {
    if (sqlTermsMapper.isArray(predicate.getKey())) {
      builder.append(
          String.format(
              ARRAY_LIKE_FN.apply(sqlTermsMapper.getTermArray(predicate.getKey())),
              predicate.getValue().replaceAll("'", "\\\\'"),
              predicate.isMatchCase()));
    } else {
      // Replace % → \% and _ → \_
      // Then replace * → % and ? → _
      LikePredicate<S> likePredicate =
          new LikePredicate<>(
              predicate.getKey(),
              predicate
                  .getValue()
                  .replace("%", "\\%")
                  .replace("_", "\\_")
                  .replace('*', '%')
                  .replace('?', '_'),
              predicate.isMatchCase());

      visitSimplePredicate(likePredicate, LIKE_OPERATOR);
    }
  }

  public void visit(NotPredicate predicate) throws QueryBuildingException {
    builder.append(NOT_OPERATOR);
    visit(predicate.getPredicate());
  }

  public void visit(IsNotNullPredicate<S> predicate) throws QueryBuildingException {
    if (isHumboldtTaxonParameter(predicate.getParameter())) {
      appendHumboldtTaxonUnary(
          getChecklistKey(predicate.getChecklistKey()), IS_NOT_NULL_ARRAY_OPERATOR);
    } else if (isSQLArray(predicate.getParameter())
        || SQLColumnsUtils.isVocabulary(term(predicate.getParameter()))) {
      builder.append(
          String.format(IS_NOT_NULL_ARRAY_OPERATOR, toSQLField(predicate.getParameter(), true)));
    } else if (TAXON_SEARCH_PARAMETERS.contains(predicate.getParameter())) {
      appendTaxonKeyIsNotNull(predicate);
    } else if (predicate.getParameter() == OccurrenceSearchParameter.GADM_GID) {
      appendUnaryList(GADM_GIDS, IS_NOT_NULL_OPERATOR);
    } else {
      // matchCase: Avoid adding an unnecessary "lower()" when just testing for null.
      builder.append(toSQLField(predicate.getParameter(), true));
      builder.append(IS_NOT_NULL_OPERATOR);
    }
  }

  public void visit(IsNullPredicate<S> predicate) throws QueryBuildingException {
    if (TAXON_SEARCH_PARAMETERS.contains(predicate.getParameter())) {
      appendTaxonKeyNull(predicate);
    } else if (isHumboldtTaxonParameter(predicate.getParameter())) {
      appendHumboldtTaxonUnary(
          getChecklistKey(predicate.getChecklistKey()), IS_NULL_ARRAY_OPERATOR);
    } else if (predicate.getParameter() == OccurrenceSearchParameter.GADM_GID) {
      appendUnaryList(GADM_GIDS, IS_NULL_OPERATOR);
    } else {
      // matchCase: Avoid adding an unnecessary "lower()" when just testing for null.
      if (isSQLArray(predicate.getParameter())) {
        builder.append(
            String.format(IS_NULL_ARRAY_OPERATOR, toSQLField(predicate.getParameter(), true)));
      } else {
        builder.append(toSQLField(predicate.getParameter(), true));
        builder.append(IS_NULL_OPERATOR);
      }
    }
  }

  /** Searches any of the NUB keys in Hive of any rank. */
  private void appendTaxonKeyIsNotNull(IsNotNullPredicate<S> predicate) {
    builder.append('(');
    builder.append(
        String.format(
            "classificationdetails['%s']['%s'] != ''",
            getChecklistKey(predicate.getChecklistKey()),
            predicate.getParameter().name().replace("_", "").toLowerCase()));
    builder.append(')');
  }

  /** Searches any of the NUB keys in Hive of any rank. */
  private void appendTaxonKeyNull(IsNullPredicate<S> predicate) {
    builder.append('(');
    builder.append(
        String.format(
            "classificationdetails['%s']['%s'] = ''",
            getChecklistKey(predicate.getChecklistKey()),
            predicate.getParameter().name().replace("_", "").toLowerCase()));
    builder.append(')');
  }

  public void visit(WithinPredicate within) throws QueryBuildingException {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;

    WKTReader reader =
        new WKTReader(spatialContextFactory.newSpatialContext(), spatialContextFactory);

    try {
      // the geometry must be valid - it was validated in the predicates constructor
      Shape geometry = reader.parse(within.getGeometry());

      builder.append('(');
      String withinGeometry;

      // Add an additional filter to a bounding box around any shapes that aren't quadrilaterals, to
      // speed up the query.
      if (geometry instanceof JtsGeometry
          && ((JtsGeometry) geometry).getGeom().getNumPoints() != 5) {
        Geometry g = ((JtsGeometry) geometry).getGeom();
        // Use the Spatial4J-fixed geometry; this is split into a multipolygon if it crosses the
        // antimeridian.
        withinGeometry = g.toText();

        Rectangle bounds = geometry.getBoundingBox();
        boundingBox(bounds);
        builder.append(CONJUNCTION_OPERATOR);

        // A tool (R?) can generate hundreds of tiny areas spread across the globe, all in a single
        // multipolygon.
        // Add bounding boxes for these too.
        // Example: https://www.gbif.org/occurrence/download/0187894-210914110416597
        if (g instanceof MultiPolygon && g.getNumGeometries() > 2) {
          builder.append("((");
          for (int i = 0; i < g.getNumGeometries(); i++) {
            if (i > 0) {
              // Too many clauses exceeds Hive's query parsing stack.
              if (i % 500 == 0) {
                builder.append(')');
                builder.append(DISJUNCTION_OPERATOR);
                builder.append('(');
              } else {
                builder.append(DISJUNCTION_OPERATOR);
              }
            }
            Geometry gi = g.getGeometryN(i);
            Envelope env = gi.getEnvelopeInternal();
            boundingBox(
                new RectangleImpl(
                    env.getMinX(),
                    env.getMaxX(),
                    env.getMinY(),
                    env.getMaxY(),
                    geometry.getContext()));
          }
          builder.append("))");
          builder.append(CONJUNCTION_OPERATOR);
        }
      } else {
        withinGeometry = within.getGeometry();
      }
      builder
          .append("contains('")
          .append(withinGeometry)
          .append("', ")
          .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLatitude))
          .append(", ")
          .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLongitude));
      // Without the "= TRUE", the expression may evaluate to TRUE or FALSE for all records,
      // depending
      // on the data format (ORC, Avro, Parquet, text) of the table (!).
      // We could not reproduce the issue on our test cluster, so it seems safest to include this.
      builder.append(") = TRUE").append(')');
    } catch (Exception e) {
      throw new QueryBuildingException(e);
    }
  }

  public void visit(GeoDistancePredicate geoDistance) throws QueryBuildingException {
    builder
        .append("(geoDistance(")
        .append(geoDistance.getGeoDistance().getLatitude())
        .append(", ")
        .append(geoDistance.getGeoDistance().getLongitude())
        .append(", '")
        .append(geoDistance.getGeoDistance().getDistance().toString())
        .append("', ")
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLatitude))
        .append(", ")
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLongitude))
        .append(") = TRUE)");
  }

  /**
   * Given a bounding box, generates greater than / lesser than queries using decimalLatitude and
   * decimalLongitude to form a bounding box.
   */
  private void boundingBox(Rectangle bounds) {
    builder
        .append('(')
        // Latitude is easy:
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLatitude))
        .append(GREATER_THAN_EQUALS_OPERATOR)
        .append(bounds.getMinY())
        .append(CONJUNCTION_OPERATOR)
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLatitude))
        .append(LESS_THAN_EQUALS_OPERATOR)
        .append(bounds.getMaxY())
        .append(CONJUNCTION_OPERATOR)

        // Longitude must take account of crossing the antimeridian:
        .append('(')
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLongitude))
        .append(GREATER_THAN_EQUALS_OPERATOR)
        .append(bounds.getMinX());
    if (bounds.getMinX() < bounds.getMaxX()) {
      builder.append(CONJUNCTION_OPERATOR);
    } else {
      builder.append(DISJUNCTION_OPERATOR);
    }
    builder
        .append(SQLColumnsUtils.getSQLQueryColumn(DwcTerm.decimalLongitude))
        .append(LESS_THAN_EQUALS_OPERATOR)
        .append(bounds.getMaxX())
        .append(')')
        .append(')');
  }

  /**
   * Builds a list of predicates joined by 'op' statements. The final statement will look like this:
   *
   * <p>
   *
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  public void visitCompoundPredicate(CompoundPredicate predicate, String op)
      throws QueryBuildingException {
    builder.append('(');
    Iterator<Predicate> iterator = predicate.getPredicates().iterator();
    while (iterator.hasNext()) {
      Predicate subPredicate = iterator.next();
      builder.append('(');
      visit(subPredicate);
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(op);
      }
    }
    builder.append(')');
  }

  public void visitSimplePredicate(SimplePredicate<S> predicate, String op)
      throws QueryBuildingException {
    if (Number.class.isAssignableFrom(predicate.getKey().type())) {
      if (SearchTypeValidator.isNumericRange(predicate.getValue())) {
        if (Integer.class.equals(predicate.getKey().type())) {
          visit(
              toIntegerRangePredicate(
                  SearchTypeValidator.parseIntegerRange(predicate.getValue()), predicate.getKey()));
        } else {
          visit(
              toNumberRangePredicate(
                  SearchTypeValidator.parseDecimalRange(predicate.getValue()), predicate.getKey()));
        }
        return;
      }
    }

    if (sqlTermsMapper.includeNullInPredicate(predicate)) {
      String column = SQLColumnsUtils.getSQLColumn(term(predicate.getKey()));
      builder
          .append('(')
          .append(toSQLField(predicate.getKey(), predicate.isMatchCase()))
          .append(op)
          .append(toSQLValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()))
          .append(DISJUNCTION_OPERATOR)
          .append(
              isSQLArray(predicate.getKey())
                  ? String.format(IS_NOT_NULL_ARRAY_OPERATOR, column)
                  : column + IS_NULL_OPERATOR)
          .append(')');
    } else {
      builder
          .append(toSQLField(predicate.getKey(), predicate.isMatchCase()))
          .append(op)
          .append(toSQLValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()));
    }
  }

  public void visitSimplePredicate(SimplePredicate<S> predicate, String op, String value)
      throws QueryBuildingException {
    if (sqlTermsMapper.includeNullInPredicate(predicate)) {
      String column = SQLColumnsUtils.getSQLColumn(term(predicate.getKey()));
      builder
          .append('(')
          .append(toSQLField(predicate.getKey(), predicate.isMatchCase()))
          .append(op)
          .append(toSQLValue(predicate.getKey(), value, predicate.isMatchCase()))
          .append(DISJUNCTION_OPERATOR)
          .append(
              isSQLArray(predicate.getKey())
                  ? String.format(IS_NOT_NULL_ARRAY_OPERATOR, column)
                  : column + IS_NULL_OPERATOR)
          .append(')');
    } else {
      builder
          .append(toSQLField(predicate.getKey(), predicate.isMatchCase()))
          .append(op)
          .append(toSQLValue(predicate.getKey(), value, predicate.isMatchCase()));
    }
  }

  /** Determines if the parameter type is a Hive array. */
  protected boolean isSQLArray(S parameter) {
    return SQLColumnsUtils.getSQLType(term(parameter)).startsWith(SQL_ARRAY_PRE);
  }

  /** Term associated to a search parameter */
  public Term term(S parameter) {
    return sqlTermsMapper.term(parameter);
  }

  private void appendHumboldtTaxonFilter(String checklistKey, S parameter, String value) {
    Objects.requireNonNull(checklistKey);

    String field = HUMBOLDT_TAXON_COLUMNS.getOrDefault(parameter, null);

    if (field == null) {
      return;
    }

    builder
        .append('(')
        .append(
            String.format(
                "stringArrayContains(%s['%s']['%s'], '%s', true)",
                SQLColumnsUtils.getSQLQueryColumn(EcoTerm.targetTaxonomicScope),
                getChecklistKey(checklistKey),
                field,
                value))
        .append(')');
  }

  private void appendHumboldtTaxonFilter(EqualsPredicate<S> taxonPredicate) {
    appendHumboldtTaxonFilter(
        getChecklistKey(taxonPredicate.getChecklistKey()),
        taxonPredicate.getKey(),
        taxonPredicate.getValue());
  }

  private void appendHumboldtTaxonFilter(InPredicate<S> taxonPredicate) {
    String field = HUMBOLDT_TAXON_COLUMNS.getOrDefault(taxonPredicate.getKey(), null);

    if (field == null) {
      return;
    }

    Set<String> taxonKeys =
        taxonPredicate.getValues().stream()
            .map(v -> toSQLValue(taxonPredicate.getKey(), v, true))
            .collect(Collectors.toSet());

    builder
        .append('(')
        .append(
            String.format(
                "arrays_overlap(%s['%s']['%s'], array(%s))",
                SQLColumnsUtils.getSQLQueryColumn(EcoTerm.targetTaxonomicScope),
                getChecklistKey(taxonPredicate.getChecklistKey()),
                field,
                String.join(",", taxonKeys)))
        .append(')');
  }

  private void appendHumboldtTaxonUnary(String checklistKey, String unaryOperator) {
    Objects.requireNonNull(checklistKey);
    builder.append('(');
    builder.append(
        String.format(
            "%s['%s']['%s'] " + unaryOperator,
            SQLColumnsUtils.getSQLQueryColumn(EcoTerm.targetTaxonomicScope),
            getChecklistKey(checklistKey),
            "usageName"));
    builder.append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
   *
   * @param taxonKeyPredicate to append as filter
   */
  private void appendTaxonomicArrayFilter(EqualsPredicate<S> taxonKeyPredicate, Term term) {

    builder
        .append('(')
        .append(
            String.format(
                "stringArrayContains(%s['%s'], '%s', true)",
                SQLColumnsUtils.getSQLQueryColumn(term),
                getChecklistKey(taxonKeyPredicate.getChecklistKey()),
                taxonKeyPredicate.getValue()))
        .append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
   *
   * @param taxonKeyPredicate to append as filter
   */
  private void appendTaxonomicSingleValueFilter(EqualsPredicate<S> taxonKeyPredicate, Term term) {

    builder
        .append('(')
        .append(
            String.format(
                "%s['%s'] = '%s'",
                SQLColumnsUtils.getSQLQueryColumn(term),
                getChecklistKey(taxonKeyPredicate.getChecklistKey()),
                taxonKeyPredicate.getValue()))
        .append(')');
  }

  /** Creates a disjunction of all the given terms. */
  private void appendGadmFilterList(List<? extends Term> terms, String value) {
    builder
        .append('(')
        .append(
            terms.stream()
                .map(
                    term ->
                        SQLColumnsUtils.getSQLQueryColumn(term)
                            + EQUALS_OPERATOR
                            + "'"
                            + value
                            + "'")
                .collect(Collectors.joining(DISJUNCTION_OPERATOR)))
        .append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank, for multiple keys.
   *
   * @param taxonomicPredicate to append as filter
   */
  private void appendTaxonomicArrayFilter(InPredicate<S> taxonomicPredicate, Term term) {
    Set<String> taxonKeys =
        taxonomicPredicate.getValues().stream()
            .map(v -> toSQLValue(taxonomicPredicate.getKey(), v, true))
            .collect(Collectors.toSet());

    builder
        .append('(')
        .append(
            String.format(
                "arrays_overlap(%s['%s'], array(%s))",
                SQLColumnsUtils.getSQLQueryColumn(term),
                getChecklistKey(taxonomicPredicate.getChecklistKey()),
                String.join(",", taxonKeys)))
        .append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank, for multiple keys.
   *
   * @param taxonomicPredicate to append as filter
   */
  private void appendTaxonomicSingleValueFilter(InPredicate<S> taxonomicPredicate, Term term) {

    Collection<String> taxonKeys = taxonomicPredicate.getValues();

    builder.append('(');
    boolean first = true;
    for (String taxonKey : taxonKeys) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder
          .append('(')
          .append(
              String.format(
                  "%s['%s'] = '%s'",
                  SQLColumnsUtils.getSQLQueryColumn(term),
                  getChecklistKey(taxonomicPredicate.getChecklistKey()),
                  taxonKey))
          .append(')');
      first = false;
    }
    builder.append(')');
  }

  /**
   * Searches every level of GADM GID in Hive for multiple keys.
   *
   * @param gadmGids to append as filter
   */
  private void appendGadmGidFilter(Collection<String> gadmGids) {
    builder.append('(');
    boolean first = true;
    for (Term term : GADM_GIDS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(SQLColumnsUtils.getSQLQueryColumn(term)).append(IN_OPERATOR).append('(');
      Iterator<String> iterator = gadmGids.iterator();
      while (iterator.hasNext()) {
        // Hardcoded GADM_LEVEL_0_GID since the type of all these parameters is the same.
        // Using .toUpperCase() is safe, GIDs must be ASCII anyway.
        builder.append(
            toSQLValue(sqlTermsMapper.getDefaultGadmLevel(), iterator.next().toUpperCase(), true));
        if (iterator.hasNext()) {
          builder.append(", ");
        }
      }
      builder.append(")");
      first = false;
    }
    builder.append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
   *
   * @param unaryOperator to append as filter
   */
  private void appendUnaryList(List<? extends Term> terms, String unaryOperator) {
    builder.append('(');
    builder.append(
        terms.stream()
            .map(term -> SQLColumnsUtils.getSQLQueryColumn(term) + unaryOperator)
            .collect(Collectors.joining(CONJUNCTION_OPERATOR)));
    builder.append(')');
  }

  /**
   * Converts decimal range into a predicate with the form: field >= range.lower AND field <=
   * range.upper.
   */
  private Predicate toNumberRangePredicate(Range<Double> range, S key) {
    if (!range.hasLowerBound()) {
      return new LessThanOrEqualsPredicate<S>(
          key, String.valueOf(range.upperEndpoint().doubleValue()));
    }
    if (!range.hasUpperBound()) {
      return new GreaterThanOrEqualsPredicate<S>(
          key, String.valueOf(range.lowerEndpoint().doubleValue()));
    }

    List<Predicate> predicates =
        List.of(
            new GreaterThanOrEqualsPredicate<S>(
                key, String.valueOf(range.lowerEndpoint().doubleValue())),
            new LessThanOrEqualsPredicate<S>(
                key, String.valueOf(range.upperEndpoint().doubleValue())));
    return new ConjunctionPredicate(predicates);
  }

  /**
   * Converts integer range into a predicate with the form: field >= range.lower AND field <=
   * range.upper.
   */
  private Predicate toIntegerRangePredicate(Range<Integer> range, S key) {
    if (!range.hasLowerBound()) {
      return new LessThanOrEqualsPredicate<S>(
          key, String.valueOf(range.upperEndpoint().intValue()));
    }
    if (!range.hasUpperBound()) {
      return new GreaterThanOrEqualsPredicate<S>(
          key, String.valueOf(range.lowerEndpoint().intValue()));
    }

    List<Predicate> predicates =
        List.of(
            new GreaterThanOrEqualsPredicate<S>(
                key, String.valueOf(range.lowerEndpoint().intValue())),
            new LessThanOrEqualsPredicate<S>(
                key, String.valueOf(range.upperEndpoint().intValue())));
    return new ConjunctionPredicate(predicates);
  }

  private void visit(Object object) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass()});
    } catch (NoSuchMethodException e) {
      log.warn(
          "Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object);
    } catch (IllegalAccessException e) {
      log.error(
          "This error shouldn't occur if all visit methods are public. Probably a programming error",
          e);
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      log.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }

  private boolean isHumboldtTaxonParameter(S parameter) {
    return HUMBOLDT_TAXON_COLUMNS.containsKey(parameter);
  }

  private String getChecklistKey(String checklistKey) {
    return !isNullOrEmpty(checklistKey) ? checklistKey : defaultChecklistKey;
  }

  public static boolean isNullOrEmpty(@Nullable String string) {
    return string == null || string.isEmpty();
  }
}
