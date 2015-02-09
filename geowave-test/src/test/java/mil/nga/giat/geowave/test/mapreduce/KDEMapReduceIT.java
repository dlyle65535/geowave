package mil.nga.giat.geowave.test.mapreduce;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;

import mil.nga.giat.geowave.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.analytics.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.raster.resize.RasterTileResizeJobRunner;
import mil.nga.giat.geowave.raster.resize.RasterTileResizeReducer;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.types.gpx.GpxUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.geometry.GeneralEnvelope;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;

public class KDEMapReduceIT extends
		MapReduceTestEnvironment
{
	private static final String TEST_COVERAGE_NAME_PREFIX = "TEST_COVERAGE";
	private static final String TEST_RESIZE_COVERAGE_NAME_PREFIX = "TEST_RESIZE";
	private static final int MAX_TILE_SIZE_POWER_OF_2 = 4;
	private static final int BASE_MIN_LEVEL = 14;
	private static final int BASE_MAX_LEVEL = 16;
	private void testIngestShp(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
//		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -t geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
	}
	@Test
	public void testKDEAndRasterResize()
			throws Exception {
		accumuloOperations.deleteAll();
		testIngestShp(
				IndexType.SPATIAL_VECTOR,
				"C:\\Users\\rfecher\\DotMatrixWorkspace\\data\\Export_Dots.shp");
		for (int i = 1; i <= 2; i++) {
//			if (i <1){
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new KDEJobRunner(),
					new String[] {
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						"Export_Dots",
						new Integer(
								BASE_MAX_LEVEL - i).toString(),
						new Integer(
								BASE_MAX_LEVEL - i).toString(),
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						tileSizeCoverageName,
						hdfs,
						jobtracker,
						TEST_NAMESPACE,
						new Integer(
								(int) Math.pow(
										2,
										i)).toString()
					});
//			}
		}
		
		final int[] counts1 = testCounts(
				TEST_COVERAGE_NAME_PREFIX,
				2,
				new Rectangle(
						256,
						256));

//		final Connector conn = ConnectorPool.getInstance().getConnector(
//				zookeeper,
//				accumuloInstance,
//				accumuloUser,
//				accumuloPassword);
//		conn.tableOperations().compact(
//				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
//				null,
//				null,
//				true,
//				true);
//		final int[] counts2 = testCounts(
//				TEST_COVERAGE_NAME_PREFIX,
//				MAX_TILE_SIZE_POWER_OF_2 + 1,
//				new Rectangle(
//						256,
//						256));

		for (int i = 1; i <= 2; i++) {
//			if (i <1){
			final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new RasterTileResizeJobRunner(),
					new String[] {
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						originalTileSizeCoverageName,
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						hdfs,
						jobtracker,
						resizeTileSizeCoverageName,
						TEST_NAMESPACE,
						new Integer(
								(int) Math.pow(
										2,
										5-i)).toString()
					});
//			}
		}
		for (Entry<Byte, Long> e : RasterTileResizeReducer.countsPerTier.entrySet()){
			System.err.println("Count at " + e.getKey()+": " + e.getValue());
		}
//
		final int[] counts3 = testCounts(
				TEST_RESIZE_COVERAGE_NAME_PREFIX,
				2 ,
				new Rectangle(
						256,
						256));

//		conn.tableOperations().compact(
//				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
//				null,
//				null,
//				true,
//				true);
//
//		final int[] counts4 = testCounts(
//				TEST_RESIZE_COVERAGE_NAME_PREFIX,
//				MAX_TILE_SIZE_POWER_OF_2 + 1,
//				new Rectangle(
//						64,
//						64));

		System.err.println("testing kde");
		for (int i = 0; i < counts1.length; i++) {
			System.err.println("counts[" + i + "]:" + counts1[i]);
		}
		System.err.println("testing resize");
		for (int i = 0; i < counts3.length; i++) {
			System.err.println("counts[" + i + "]:" + counts3[i]);
		}
	}

	private static int[] testCounts(
			final String coverageNamePrefix,
			final int numCoverages,
			final Rectangle pixelDimensions )
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.createConfig(
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						false,
						Interpolation.INTERP_NEAREST));
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
//					-71.12,
						52.5,
//					42.38
						29.58
//						-72,
//						42
				},
				new double[] {
//					-71.07,
						52.57,
						29.65
//					42.4
//						-71,
//						43
				});
		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);
		final Raster[] rasters = new Raster[numCoverages];
		final int[] counts = new int[numCoverages];
		for (int i = 0; i < numCoverages; i++) {
			final String tileSizeCoverageName = coverageNamePrefix + (i+1);
			RasterUtils.COVERAGE_NAME = coverageNamePrefix + "_" + i +"_";
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					pixelDimensions,
					queryEnvelope,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[i] = raster;
			final File f = new File(
					"C:\\Temp\\kde_test4",
					coverageNamePrefix + "_" + i + ".png");
			f.delete();
			f.createNewFile();
			BufferedImage heatmap = new BufferedImage(rasters[i].getWidth(), rasters[i].getHeight(), BufferedImage.TYPE_BYTE_GRAY);
			Graphics g = heatmap.createGraphics();
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								2);
						if (!Double.isNaN(sample)) {
							g.setColor(new Color((float)sample, (float)sample,(float)sample));
							g.fillRect(x, y, 1, 1);
						}
				}
			}
			heatmap.flush();
			ImageIO.write(
					heatmap,
					"png",
					f);
		}
		for (int i = 0; i < numCoverages; i++) {
			counts[i] = 0;
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (!Double.isNaN(sample)) {
							counts[i]++;
						}
					}
				}
			}
		}
		// make sure all of the counts are the same before and after compaction
		// for (int i = 1; i < counts.length; i++) {
		// Assert.assertEquals(
		// "The count of non-nodata values is different between the 1 pixel KDE and the 2^"
		// + i + " pixel KDE",
		// counts[0],
		// counts[i]);
		// }
		return counts;
	}
}
