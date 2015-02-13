package mil.nga.giat.geowave.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.IngestEntryInfo;

abstract public class TimeRangeDataStatistics<T> extends
		AbstractDataStatistics<T>
{
	public final static ByteArrayId STATS_ID = new ByteArrayId(
			"TIME_RANGE");

	private double min = Double.MAX_VALUE;
	private double max = 0;

	protected TimeRangeDataStatistics() {
		super();
	}

	public TimeRangeDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId);
	}

	public boolean isSet() {
		if ((min == Double.MAX_VALUE) || (max == 0)) {
			return false;
		}
		return true;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getWidth() {
		return max - min;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.putDouble(min);
		buffer.putDouble(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		min = buffer.getDouble();
		max = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final IngestEntryInfo entryInfo,
			final T entry ) {
		final NumericRange range = getRange(entry);
		if (range != null) {
			min = Math.min(
					min,
					range.getMin());
			max = Math.max(
					max,
					range.getMax());
		}
	}

	abstract protected NumericRange getRange(
			final T entry );

	@Override
	public ByteArrayId getStatisticsId() {
		return STATS_ID;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof TimeRangeDataStatistics)) {
			final TimeRangeDataStatistics<T> stats = (TimeRangeDataStatistics<T>) statistics;
			if (stats.isSet()) {
				min = Math.min(
						min,
						stats.getMin());
				max = Math.max(
						max,
						stats.getMax());
			}
		}
	}
}
