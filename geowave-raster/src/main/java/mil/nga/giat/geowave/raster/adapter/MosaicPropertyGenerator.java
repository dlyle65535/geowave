package mil.nga.giat.geowave.raster.adapter;

import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;

import javax.media.jai.JAI;
import javax.media.jai.OperationRegistry;
import javax.media.jai.PropertyGenerator;
import javax.media.jai.RenderedOp;
import javax.media.jai.operator.MosaicDescriptor;
import javax.media.jai.operator.WarpDescriptor;

import org.geotools.resources.coverage.CoverageUtilities;

import com.sun.media.jai.opimage.MosaicRIF;
import com.sun.media.jai.util.PropertyGeneratorImpl;

public class MosaicPropertyGenerator extends
		PropertyGeneratorImpl
{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public MosaicPropertyGenerator() {
		super(
				new String[] {
					"sourceThreshold"
				},
				new Class[] {
					double[][].class
				},
				new Class[] {
					RenderedOp.class
				});
	}

	static boolean registered = false;

	public synchronized static void register(
			final boolean force ) {
		if (!registered || force) {
			final OperationRegistry registry = JAI.getDefaultInstance().getOperationRegistry();
			registry.addPropertyGenerator(
					"rendered",
					"Mosaic",
					new MosaicPropertyGenerator());
			registry.unregisterDescriptor(new MosaicDescriptor());
			registry.registerDescriptor(new SourceThresholdMosaicDescriptor());
//			registry.unregisterFactory("rendered", "mosaic", "com.sun.media.jai", new MosaicRIF());
			registry.registerFactory("rendered", "mosaic", "com.sun.media.jai", new MosaicRIF());
//			rendered    com.sun.media.jai.opimage.MosaicRIF		com.sun.media.jai	mosaic		sunmosaicrif
			registered = true;
		}
	}

	@Override
	public Object getProperty(
			final String name,
			final Object opNode ) {
		validate(
				name,
				opNode);

		if ((opNode instanceof RenderedOp) && name.equalsIgnoreCase("sourceThreshold")) {
			final RenderedOp op = (RenderedOp) opNode;

			final ParameterBlock pb = op.getParameterBlock();

			// Retrieve the rendered source image and its ROI.
			final RenderedImage src = pb.getRenderedSource(0);
			final Object property = src.getProperty("sourceThreshold");
			if (property != null) {
				return property;
			} // Getting the Threshold to use
			final double threshold = CoverageUtilities.getMosaicThreshold(src.getSampleModel().getDataType());
			// Setting the Threshold object for the mosaic
			return new double[][] {
				{
					threshold
				}
			};
		}
		return java.awt.Image.UndefinedProperty;
	}

}
