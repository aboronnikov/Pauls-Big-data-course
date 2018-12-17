package com.epam.program;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

/**
 * An abstraction of a line that contains only the important fields.
 */
public class Line {

    /**
     * Default logger.
     */
    private static final Log log = LogFactory.getLog(Line.class);

    /**
     * Number of adults. For our task, should be 2.
     */
    private String adultCount;

    /**
     * Name of continent. Part of compound key.
     */
    private String continent;

    /**
     * Name of country. Part of compound key.
     */
    private String country;

    /**
     * Name of market. Part of compound key.
     */
    private String market;

    /**
     * Constructor that extracts only the necessary fields from a line.
     *
     * @param line an abstraction of a line.
     */
    public Line(String line) {
        try {
            final int ADULT_COUNT_ID = 14;
            final int CONTINENT_ID = 19;
            final int COUNTRY_ID = 20;
            final int MARKET_ID = 21;

            String[] data = line.split(",");
            adultCount = data[ADULT_COUNT_ID];
            continent = data[CONTINENT_ID];
            country = data[COUNTRY_ID];
            market = data[MARKET_ID];
        } catch (ArrayIndexOutOfBoundsException e) {
            log.error("Please check the validity of your data", e);
        }
    }

    /**
     * Checks if this booking entry is for a couple.
     *
     * @return true of false, indicating if this is a couple.
     */
    public boolean isCouple() {
        return adultCount.equals("2");
    }

    /**
     * This is necessary because I aggregate my result into a map.
     *
     * @param obj other object to be compared with this one.
     * @return true of false, indicating equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Line)) return false;
        Line that = (Line) obj;
        return this.adultCount.equals(that.adultCount) &&
                this.continent.equals(that.continent) &&
                this.country.equals(that.country) &&
                this.market.equals(that.market);
    }

    /**
     * This is necessary because I aggregate my result into a map.
     *
     * @return the hashcode on this object.
     * @see <a href="https://commons.apache.org/proper/commons-lang/javadocs/api-3.1/org/apache/commons/lang3/builder/HashCodeBuilder.html">
     * https://commons.apache.org/proper/commons-lang/javadocs/api-3.1/org/apache/commons/lang3/builder/HashCodeBuilder.html
     * <a/>
     */
    @Override
    public int hashCode() {
        final int INITIAL_ODD_NUMBER = 17;
        final int MULTIPLIER_ODD_NUMBER = 37;

        return new HashCodeBuilder(INITIAL_ODD_NUMBER, MULTIPLIER_ODD_NUMBER)
                .append(market)
                .append(country)
                .append(continent)
                .append(adultCount)
                .toHashCode();
    }
}