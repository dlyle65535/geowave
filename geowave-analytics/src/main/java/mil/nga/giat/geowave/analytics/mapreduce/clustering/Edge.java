package mil.nga.giat.geowave.analytics.mapreduce.clustering;

public class Edge
{
	public com.vividsolutions.jts.geom.Coordinate pt1;
	public com.vividsolutions.jts.geom.Coordinate pt2;
	public int count = 0;
	private final double tol = 10e-12;

	public Edge(
			final com.vividsolutions.jts.geom.Coordinate pt1,
			final com.vividsolutions.jts.geom.Coordinate pt2 ) {
		this.pt1 = pt1;
		this.pt2 = pt2;
	}

	public void tallyEdge(
			final com.vividsolutions.jts.geom.Coordinate pt1,
			final com.vividsolutions.jts.geom.Coordinate pt2 ) {
		if (((this.pt1.distance(pt1) < tol) && (this.pt2.distance(pt2) < tol)) || ((this.pt1.distance(pt2) < tol) && (this.pt2.distance(pt1) < tol))) {
			count++;
		}
	}

}
