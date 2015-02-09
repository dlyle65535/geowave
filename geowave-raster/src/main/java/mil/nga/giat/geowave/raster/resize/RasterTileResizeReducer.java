package mil.nga.giat.geowave.raster.resize;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;
	public static Map<Byte, Long> countsPerTier = new HashMap<Byte, Long>();
	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					helper.getGeoWaveOutputKey(),
					mergedCoverage);
		}
		synchronized (countsPerTier){
			Long tier = countsPerTier.get(key.getDataId().getBytes()[0]);
			if (tier == null){
				tier = 0L;
			}
			Raster raster = mergedCoverage.getRenderedImage().getData();
			for (int x = 0; x < raster.getWidth(); x++) {
				for (int y = 0; y < raster.getHeight(); y++) {
					final double sample = raster.getSampleDouble(
							x,
							y,
							2);
					if (!Double.isNaN(sample)) {
						tier++;
					}
				}
			}
			countsPerTier.put(key.getDataId().getBytes()[0], tier);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
