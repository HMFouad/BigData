package bigdata;

public class ZoomPays {
	public String pays;
	public double latitude;
	public double longitude;
	public double population;

	public ZoomPays(String pays, double latitude, double longitude, double population) {
		super();
		this.pays = pays;
		this.latitude = latitude;
		this.longitude = longitude;
		this.population = population;
	}

	@Override
	public String toString() {
		return "zoomCordonnees [pays=" + pays + ", latitude=" + latitude + ","+ ", longitude=" + longitude + ","+ ", population=" + population + ","+"]";
	}
}
