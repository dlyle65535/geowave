package mil.nga.giat.geowave.store.adapter.statistics;

import java.util.Date;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.query.TemporalRange;

abstract public class TimeRangeDataStatistics<T> extends
		NumericRangeDataStatistics<T>
{
	public final static ByteArrayId STATS_ID = new ByteArrayId(
			"TIME_RANGE");

	protected TimeRangeDataStatistics() {
		super();
	}

	public TimeRangeDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId);
	}

	@Override
	public ByteArrayId getStatisticsId() {
		return STATS_ID;
	}

	public TemporalRange asTemporalRange() {
		return new TemporalRange(
				new Date(
						(long) this.getMin()),
				new Date(
						(long) this.getMax()));
	}
}
