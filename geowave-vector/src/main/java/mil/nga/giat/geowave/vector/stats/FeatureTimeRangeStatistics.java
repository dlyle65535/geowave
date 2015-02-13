package mil.nga.giat.geowave.vector.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.store.adapter.statistics.TimeRangeDataStatistics;
import mil.nga.giat.geowave.store.dimension.Time;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureTimeRangeStatistics extends
		TimeRangeDataStatistics<SimpleFeature>
{
	private IndexFieldHandler<SimpleFeature, Time, Object> indexHandler;

	protected FeatureTimeRangeStatistics() {
		super();
	}

	public FeatureTimeRangeStatistics(
			final ByteArrayId dataAdapterId, IndexFieldHandler<SimpleFeature, Time, Object> indexHandler) {
		super(
				dataAdapterId);
		this.indexHandler = indexHandler;
	}

	@Override
	protected NumericRange getRange(
			SimpleFeature entry ) {
		Time time = indexHandler.toIndexValue(entry);
		NumericData nd =  time.toNumericData();
		if (nd instanceof NumericRange) return (NumericRange)nd;
		return new NumericRange(nd.getMin(), nd.getMax());
	}

}
